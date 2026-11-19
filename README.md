# Radar de Precios — Pipeline

Pipeline de datos de producción sobre los datos públicos del SEPA (Sistema
Electrónico de Publicidad de Precios Argentinos), con foco en prácticas de
Data Engineering: orquestación, modelado, CI/CD y exposición de datos a un
agente (Claude vía MCP).

Dashboard relacionado: [radar-precios-ar](https://github.com/jgarciarios/radar-precios-ar).

## Roadmap

1. **Orquestación** — Airflow en Docker Compose, DAG que descarga el dump
   diario del SEPA a una zona *raw*.
2. **Staging + carga (actual)** — 2a: parseo del ZIP raw a Parquet en
   `data/staging/` (este documento). 2b: carga de ese staging a BigQuery
   (capa gratuita), vía GCS como landing intermedio.
3. **Transformación** — modelos dbt (staging → intermediate → marts) con
   tests de calidad y freshness.
4. **CI/CD** — GitHub Actions corriendo lint, tests de dbt y validación de
   DAGs en cada PR.
5. **Exposición** — servidor MCP sobre BigQuery para consultar precios desde
   Claude.

## Fase 1: ingesta diaria con Airflow

### La fuente

El SEPA no publica "el archivo del 19/11": publica un archivo fijo por día
de la semana (`sepa_lunes.zip` ... `sepa_domingo.zip` en
[datos.produccion.gob.ar/dataset/sepa-precios](https://datos.produccion.gob.ar/dataset/sepa-precios))
que el Estado **sobrescribe** todos los días con los datos de ese día. No hay
catálogo histórico en esa URL: si no se guarda el archivo del día, el dato se
pierde para siempre en cuanto se reescriba la próxima semana.

Esto condiciona el diseño del DAG:

- **Sin backfill.** `catchup=False` porque no existe un archivo "del lunes
  pasado" que recuperar — pedírselo a Airflow sería pedir algo imposible de
  cumplir.
- **Reloj, no `logical_date`.** El DAG decide qué día de la semana pedir
  usando la hora real (`pendulum.now()`), no el `logical_date` que Airflow le
  asigna al run. El `logical_date` de un DAG con horario cron representa el
  *inicio* del intervalo de datos que cubre ese run, no necesariamente "hoy";
  como la fuente solo expone el snapshot del día real (no un intervalo
  histórico), guiarse por `logical_date` llevaría a pedir el archivo
  equivocado, por ejemplo en el primer run después de una falla.
- **Particionado por fecha de descarga**, no por nombre de archivo: el ZIP se
  guarda en `data/raw/sepa/dt=YYYY-MM-DD/`, así que aunque la fuente siempre
  se llame `sepa_jueves.zip`, en nuestro raw queda un snapshot por día,
  disponible para la fase 2.

### Arquitectura de la fase 1

```
┌─────────────────────┐        ┌──────────────────────────┐
│  datos.produccion    │        │   Airflow (Docker Compose) │
│  .gob.ar (SEPA)      │        │                            │
│                      │  HTTP  │  ┌──────────┐              │
│  sepa_<dia>.zip      │◄───────┼──┤ scheduler │              │
│  (se sobreescribe    │        │  │ + webserver│             │
│   todos los días)    │        │  └────┬─────┘              │
└──────────────────────┘        │       │ DAG: sepa_ingest_daily
                                 │       ▼                     │
                                 │  resolve_url → download_zip │
                                 │       → validate_download   │
                                 │       │                     │
                                 │       ▼                     │
                                 │  data/raw/sepa/dt=.../       │
                                 │    sepa_<dia>.zip (volumen)  │
                                 └───────────────────────────┘
```

El DAG tiene 3 tareas (TaskFlow API):

1. `resolve_url`: calcula qué día de la semana es "hoy" y arma la URL de
   descarga correspondiente.
2. `download_zip`: descarga el ZIP en streaming (chunks de 1MB, sin cargarlo
   entero en memoria — el archivo real pesa ~300MB) a
   `data/raw/sepa/dt=<fecha>/sepa_<dia>.zip`.
3. `validate_download`: chequea que el archivo pese más de un umbral mínimo
   (5MB — si el servidor devolvió una página de error en vez del ZIP, el
   archivo pesa poco) y que sea un ZIP válido (`zipfile.is_zipfile`).

Decisiones de infraestructura:

- **`LocalExecutor`**, no Celery/Kubernetes: con un solo DAG chico, correr
  las tareas como subprocesos del scheduler es suficiente y evita sumar
  Redis o un cluster. Es una opción real de producción para cargas chicas,
  no un atajo de juguete.
- **Imagen propia** (`Dockerfile` sobre `apache/airflow`) en vez de
  `_PIP_ADDITIONAL_REQUIREMENTS`: esa variable de entorno reinstala paquetes
  en cada arranque de contenedor y la propia documentación de Airflow la
  marca como no apta para producción. Con una imagen propia, la dependencia
  (`requests`) queda fijada en una capa de la imagen y el build es
  reproducible.
- **Reintentos**: cada tarea reintenta hasta 3 veces con 5 minutos de espera
  (`default_args`), para tolerar caídas transitorias del servidor de datos.
- **Escritura atómica (`.part` + `os.replace`)**: `download_zip` escribe a un
  archivo temporal y recién al final lo renombra al nombre definitivo. Esto
  salió de un bug real encontrado probando el DAG: con `max_active_runs=1`,
  un trigger manual quedó encolado y arrancó justo cuando el run anterior
  terminaba, pisando el ZIP final mientras otro proceso lo estaba leyendo
  para validarlo. `os.replace()` es atómico dentro del mismo filesystem, así
  que el archivo final nunca queda a medio escribir.

### Cómo correrlo

```bash
cp .env.example .env
# completar AIRFLOW_FERNET_KEY, por ejemplo con:
# python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

docker compose build
docker compose up -d

# UI en http://localhost:8080 (usuario/clave: admin/admin)
```

Para probar el DAG sin esperar al cron de las 18:00:

```bash
docker compose exec airflow-scheduler airflow dags trigger sepa_ingest_daily
```

El resultado queda en `data/raw/sepa/dt=<fecha-de-hoy>/sepa_<dia>.zip`
(carpeta fuera de git — `data/` está en `.gitignore`).

## Fase 2a: raw → staging (Parquet)

### La estructura real del ZIP

El ZIP diario no trae CSVs sueltos: trae un sub-ZIP por comercio (18 el
2026-11-19: `sepa_1_comercio-sepa-24_..._.zip`, etc.), y cada sub-ZIP trae
`comercio.csv`, `sucursales.csv` y `productos.csv` separados por `|`,
UTF-8, con fin de línea CRLF. Esto se confirmó inspeccionando el ZIP real
antes de escribir el parser — no son supuestos.

Ese mismo ZIP real trajo, sin buscarlos a propósito, los problemas de datos
que la fase 2a tiene que tolerar:

- **Un sub-ZIP de 0 bytes** (comercio 36): roto de verdad, no un caso de
  laboratorio.
- **Footer inconsistente**: los 3 CSV terminan con una línea en blanco y
  `Última actualización: <fecha>` — pero no es un literal estable. Aparece
  como `Ultima actualizacion:` (sin tilde), `Ultima Actualizacion:`
  (mayúscula distinta), con `\n` en vez de `\r\n`, y en un caso con una
  línea extra de puro byte NUL antes (comercio 47). Si se matchea por
  substring exacto en vez de una regex tolerante a mayúsculas/tildes, estos
  casos se leen como "no tiene footer" en vez de "tiene footer con otro
  formato" — hay que elegir explícitamente contra cuál de los dos errores
  equivocarse.
- **Comercios desactualizados**: el footer de `comercio.csv` es la fecha
  real en que ese comercio generó su archivo. El comercio 47 no se
  actualiza desde **2025-06-11** — su sub-ZIP de hoy tiene datos de hace
  casi un año y medio, con el mismo nombre de archivo que si fuera de hoy.
- **Precios de referencia negativos** (comercio 15, 969 filas en
  `-$28.447` a `-$31.042`): error de carga del comercio, no un artefacto
  del parseo — confirmado mirando las filas crudas.
- Dato curioso que *no* viola ninguna regla pedida: precio `$0` en
  productos marcados `DISCONTINUO`, y precio `$9.999.999` en SKUs de
  `GENERICO` tipo "EXIBIDOR DE PRECIOS"/"DISPLAY DOWN PART" — son sentinels
  de catálogo, no corrupción. `$12.999.000` por un Smart TV LG OLED 83" es
  un precio real (inflación argentina), no un outlier de parseo.

### Arquitectura de la fase 2a

```
data/raw/sepa/dt=.../sepa_<dia>.zip   (de la fase 1)
          │
          ▼
┌───────────────────────────────────────────────────────┐
│  pipeline/stage.py  ·  run_staging()                    │
│                                                           │
│  por cada sub-zip de comercio:                           │
│    unzip.read_comercio_csvs()  ──▶  BadZipFile/KeyError? │
│         │ ok                              │ sí           │
│         ▼                                 ▼              │
│    parse.parse_*_csv()              _quarantine/         │
│         │                           dt=....jsonl         │
│         ▼                            (comercio_id +      │
│    validate.validate_comercio() ──▶  razón, append-only  │
│         │ ok           │ ValidationError  por partición) │
│         ▼               └──────────────▶ (mismo log)     │
│    acumula en memoria                                     │
└───────────────────────────────────────────────────────┘
          │ concat de todos los comercios ok
          ▼
   tmp dir → os.rename atómico
          ▼
data/staging/{comercio,sucursales,productos}/dt=.../*.parquet
```

`pipeline/` es un paquete Python puro, sin dependencia de Airflow, probado
con un venv local (`requirements-staging.txt`) para iterar la lógica de
transformación sin reconstruir la imagen de Docker en cada cambio. Se
enchufa como task del DAG en la fase 2b.

### Decisiones de diseño

- **Aislamiento por comercio, no por archivo completo.** Cada comercio se
  procesa en su propio `try/except`: un ZIP roto, un CSV al que le falta
  una columna, o una fila con precio negativo quarantinea *ese* comercio
  (se loguea el motivo en `_quarantine/dt=.../*.jsonl`) sin frenar a los
  otros 17. La alternativa — frenar todo el día ante el primer comercio
  roto — convertiría un problema de un proveedor en una pérdida total del
  día para los otros 17.
- **Footer tolerante por regex, no por substring exacto.** Explicado arriba
  con el caso real: `[uú]ltima\s+actualizaci[oó]n` con `re.IGNORECASE` en
  vez de comparar contra un string fijo. La fecha que se extrae de ahí es,
  además, la que se usa para la regla "fecha del archivo = fecha de la
  partición" — reutilizamos un dato que la fuente ya nos da en vez de
  inventar una heurística nueva.
- **Encoding UTF-8 con fallback a latin-1.** La fuente publica UTF-8, pero
  si algún comercio manda bytes inválidos (típicamente en un campo de texto
  libre como la descripción del producto), preferimos decodificar como
  latin-1 (que nunca falla, mapea 1 a 1 cualquier byte) antes que perder el
  comercio entero por un campo de texto.
- **Escritura atómica a nivel directorio.** Mismo patrón que el `.part` +
  `os.replace()` de la fase 1, pero un nivel más arriba: se escribe a un
  directorio temporal (`tempfile.mkdtemp`) y se reemplaza con `os.rename()`
  el directorio final (`dt=.../`). `os.rename()` es atómico dentro del
  mismo filesystem, así que no hay ventana en la que el staging quede a
  medio escribir.
- **Idempotencia por reemplazo total, no por append.** Cada corrida
  recalcula las 3 tablas desde cero a partir del ZIP raw y pisa la
  partición completa del día. No hay "insertar si no existe": hay
  "recalcular y reemplazar". Esto es lo que garantiza que correr el mismo
  día dos veces no duplica filas — se probó corriendo el pipeline dos veces
  contra el ZIP real del 2026-11-19 y comparando el Parquet resultante
  (mismo conteo de filas, mismo MD5 del archivo).
- **Reglas de validación como funciones puras.** `validate_productos()` y
  `validate_fecha_archivo()` reciben DataFrames/fechas y devuelven listas
  de strings con los problemas encontrados - no tocan el filesystem ni el
  ZIP. Permite testear cada regla con pytest sin fixtures pesados, y
  `validate_comercio()` junta todos los problemas de un comercio en un solo
  error en vez de abortar en el primero, para que el log de cuarentena
  diga todo de una.
- **Fuera de alcance de 2a, a propósito:** correr esto como task de Airflow
  (sigue siendo un script local por ahora) y cargar a BigQuery. Separar
  "la lógica de transformación es correcta" de "está enchufada al
  orquestador" permite iterar la primera con pytest en segundos, en vez de
  reconstruir la imagen de Docker en cada ajuste.

### Cómo correrlo

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements-staging.txt

python -m pytest tests/ -q

python scripts/stage_sepa.py --date 2026-11-19
```

Verificación rápida con DuckDB (sin instalar nada aparte, ya está en
`requirements-staging.txt`):

```bash
python3 -c "
import duckdb
con = duckdb.connect()
print(con.execute('''
    select id_comercio, count(*) as filas
    from read_parquet(\"data/staging/productos/dt=2026-11-19/*.parquet\")
    group by id_comercio order by filas desc
''').fetchdf())
"
```

## Fase 2b, paso 1: el staging como task del DAG

El módulo `pipeline/` (fase 2a) corría como script local, fuera de
Airflow. Este paso lo enchufa como una cuarta task del DAG, sin duplicar
su lógica: la task importa `pipeline.stage.run_staging` directamente.

### Arquitectura

```
resolve_url ──▶ download_zip ──▶ validate_download ──┐
     │                                                 │ (solo orden,
     │                                                 │  sin dato)
     └─────────────────────────────────────────────────▶ stage_productos
                                                               │
                                                               ▼
                                                    pipeline.stage.run_staging()
                                                               │
                                                               ▼
                                          data/staging/{comercio,sucursales,
                                          productos}/dt=.../*.parquet
                                          + _quarantine/ _quarantine_rows/
                                            _inactive_comercios/ _quality_report/
```

`stage_productos` depende de `resolve_url` (necesita `partition_date`) y
de `validate_download` (tiene que esperar a que el ZIP esté validado, pero
no usa lo que esa task devuelve) - por eso la dependencia con
`validate_download` se fuerza con `>>` en el código en vez de pasarle el
dato, y la de `resolve_url` sale sola porque sí usa su resultado.

### Decisiones de diseño

- **Importar `pipeline/`, no copiarlo.** El `Dockerfile` ya instala
  `pandas`/`pyarrow` (igual que el venv local de la fase 2a) y
  `docker-compose.yml` monta `./pipeline` dentro del contenedor con
  `PYTHONPATH=/opt/airflow`, así el DAG hace
  `from pipeline.stage import run_staging` tal cual. La alternativa -
  pegar la lógica de parseo/validación adentro del DAG - duplicaría código
  que ya está testeado con 41 tests en `tests/`.
- **Umbral y tolerancia como `params` del DAG**, no como constantes fijas
  en el archivo del DAG: `max_bad_row_ratio` y `max_lag_days` se pueden
  cambiar desde la UI de Airflow al disparar un run ("Trigger DAG w/
  config"), con los mismos defaults que `pipeline/` (se importan las
  constantes, no se copia el número).
- **Un comercio roto no tira la task; cero comercios ok, sí.** `run_staging`
  ya maneja el aislamiento por comercio (fase 2a); `stage_productos` solo
  agrega una regla a nivel task: si `ok_comercio_ids` queda vacío, eso ya
  no es "un comercio con problemas" sino una señal de que algo más grave
  pasó con el ZIP del día entero, y ahí sí tiene sentido fallar la task
  (reintentos, alertas).
- **Idempotencia heredada, no reinventada.** `stage_productos` no agrega
  ningún mecanismo de idempotencia propio - delega enteramente en que
  `run_staging` ya escribe cada tabla/log de forma atómica (tmp dir/archivo
  + rename, fase 2a). Un retry de Airflow vuelve a correr la task entera,
  que vuelve a recalcular y pisar la partición del día: mismo resultado,
  sin acumulación. Confirmado con MD5 combinado idéntico forzando un
  retry real (`airflow tasks clear`), no solo corriendo el script dos
  veces.
- **Un Parquet por comercio, no uno gigante por tabla.** La primera
  corrida dentro de Airflow murió con exit code -9
  (`docker inspect` confirmó `OOMKilled: true`). La causa: `run_staging`
  acumulaba las ~18 tablas completas de productos (hasta ~15M filas) en
  listas de DataFrames y concatenaba todo al final, duplicando esos datos
  en memoria en el pico. El fix (ver commit correspondiente) escribe el
  archivo Parquet de cada comercio apenas se lo procesa y lo descarta
  antes de pasar al siguiente - nunca hay más de un comercio completo en
  memoria. `pd.read_parquet()` sobre el directorio sigue combinando los
  archivos de forma transparente. Vale la aclaración para la entrevista:
  esto no era un bug de lógica, era un problema de uso de memoria que solo
  aparece con volumen real de datos - correrlo contra fixtures chicos en
  los tests nunca lo iba a mostrar.

### Cómo correrlo

```bash
docker compose exec airflow-scheduler airflow dags trigger sepa_ingest_daily
```

Para cambiar los umbrales en un run puntual, desde la UI (`Trigger DAG w/
config`) o por CLI:

```bash
docker compose exec airflow-scheduler airflow dags trigger sepa_ingest_daily \
  --conf '{"max_bad_row_ratio": 0.02, "max_lag_days": 2}'
```

### Resultado de la corrida real (2026-11-19)

DAG completo, de punta a punta, contra el ZIP real del día:

```
staging 2026-11-19: 16/18 comercios ok, 1 en cuarentena, 1 inactivos,
filas escritas={'comercio': 29, 'sucursales': 3103, 'productos': 14954793}
comercio 36 en cuarentena: BadZipFile: File is not a zip file
comercio 47 inactivo hace 526 días (sin actualizar desde 2025-06-11) -
no se repite en la cuarentena diaria, ver _inactive_comercios/
```

`data/staging/productos/dt=2026-11-19/` quedó con 16 archivos Parquet (uno
por comercio aceptado) y 14.954.793 filas en total.

### Qué falta (fase 2b, paso 2 en adelante)

`data/staging/` sigue siendo Parquet local. Cargar eso a BigQuery (vía GCS
como landing intermedio) es el próximo paso de la fase 2b.
