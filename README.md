# Radar de Precios — Pipeline

[![CI](https://github.com/jgarciarios/radar-precios-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/jgarciarios/radar-precios-pipeline/actions/workflows/ci.yml)

Pipeline de datos de producción que descarga el dump diario de precios del
SEPA (supermercados de Argentina), lo transforma y lo carga a BigQuery, y
lo modela con dbt en tablas listas para analizar - con CI, tests de
calidad y trazabilidad de cada decisión de diseño sobre datos reales.

Dashboard relacionado: [radar-precios-ar](https://github.com/jgarciarios/radar-precios-ar).

## Roadmap

1. **Orquestación** ✅ — Airflow en Docker Compose, DAG que descarga el
   dump diario del SEPA a una zona *raw*.
2. **Staging + carga** ✅ — 2a: parseo del ZIP raw a Parquet en
   `data/staging/`. 2b: carga de ese staging a BigQuery (capa gratuita),
   sin GCS de por medio.
3. **Transformación** ✅ — modelos dbt (staging → marts) con tests de
   calidad.
4. **CI/CD (actual)** — 4a: GitHub Actions con lint + tests + `dbt parse`
   en cada push/PR. 4b: política de retención aplicada, prueba de acceso
   al SEPA desde runners de GitHub.
5. **Exposición** — servidor MCP sobre BigQuery para consultar precios desde
   Claude.

## Decisiones de arquitectura

Resumen de las tres decisiones que más preguntas generarían en una
revisión de este proyecto - cada una con el detalle completo más abajo,
en la fase donde se tomó.

### 1. La ingesta corre en Airflow local, no en GitHub Actions

Lo probé en vez de asumirlo: [`.github/workflows/probe-sepa.yml`](.github/workflows/probe-sepa.yml)
pide el primer MB del ZIP del SEPA desde un runner de GitHub, sin
credenciales, solo para medir si el servidor responde.

| Origen | HTTP status | Resultado |
|---|---|---|
| Local (mi máquina) | **200** | Archivo real, `Content-Range` con el tamaño completo |
| Runner de GitHub Actions | **403** | Página de error, no el ZIP |

El SEPA (`datos.produccion.gob.ar`) bloquea el request desde la IP del
runner - el patrón típico de un WAF filtrando rangos de IP conocidos de
datacenters/cloud, no un problema de nuestro código (el script armó la
URL correcta y completó la conexión en los dos casos). Por eso la ingesta
real corre en Airflow **local** (Docker Compose) y no se puede, tal como
está hoy, mover a un runner de GitHub sin un proxy o IP residencial de
por medio. Detalle completo: [fase 4b, paso 2](#fase-4b-paso-2-prueba-de-acceso-al-sepa-sin-credenciales).

### 2. Retención: 3 días para el crudo, sin límite para los marts

`sepa.productos` pesa **1,97 GB por partición diaria** - sin retención,
agota los 10 GB de la capa gratuita de BigQuery en ~5 días. Los marts de
dbt pesan 500-4.000 veces menos (6,2 MB y 1,66 MB).

Aplicado en `pipeline/bigquery_load.py::ensure_table` (no en un `ALTER
TABLE` manual, para que quede versionado y se re-aplique solo):

- `sepa.productos` / `sepa.comercio`: `time_partitioning.expiration_ms` =
  **3 días**. Barato de regenerar - todo el pipeline (fase 1-2b) es
  idempotente desde el ZIP original.
- `mart_precio_por_kilo` / `mart_brecha_precios`: **sin expiración** - son
  los que valen para historia, y a este ritmo tardarían años en pesar lo
  que el crudo pesa en una semana.

Confirmado contra la metadata real de BigQuery (no solo contra el
código): `expiration_ms` pasó de `None` a `259200000` (3,0 días exactos)
en ambas tablas, sin perder ninguna de las filas ya cargadas. Detalle
completo: [fase 4b, paso 1](#fase-4b-paso-1-política-de-retención-en-bigquery_loadpy).

### 3. No se usa `productos_precio_referencia` para normalizar precios

El SEPA trae un campo que parece ser "precio ya normalizado", pero
resultó inconsistente entre comercios: para un whisky de 750cc,
`productos_cantidad_referencia` dice literalmente "750" mientras que
`productos_precio_referencia` está calculado como si fuera "por cada
1000cc" - el campo no es confiable como divisor genérico.

**Decisión:** `precio_por_kilo` se calcula desde cero
(`precio_lista / cantidad_presentacion`, con normalización de unidad
propia), no desde `precio_referencia`. Ese campo se conserva solo para
auditoría. Detalle completo con la tabla del ejemplo:
[fase 3](#por-qué-no-usamos-productos_precio_referencia).

## Cómo correrlo (de punta a punta)

```bash
# 1. Ingesta + carga a BigQuery (fases 1-2b) - requiere .env con las
#    credenciales de GCP, ver fase 2b paso 2 para el detalle de setup.
docker compose up -d
docker compose exec airflow-scheduler airflow dags trigger sepa_ingest_daily

# 2. Transformación (fase 3) - requiere GCP_PROJECT_ID y
#    GOOGLE_APPLICATION_CREDENTIALS en el entorno.
cd dbt/radar_precios
dbt run --profiles-dir . && dbt test --profiles-dir .

# 3. CI (fase 4a) corre solo en cada push/PR - no hace falta nada local.
#    Prueba de acceso al SEPA (fase 4b, manual):
gh workflow run probe-sepa.yml
```

```
┌─────────────────────────┐
│ datos.produccion.gob.ar  │  SEPA: ZIP diario, HTTP público
└────────────┬─────────────┘       (bloqueado desde runners de GitHub)
             │
             ▼
┌──────────────────────────────────────────────────────────────┐
│ Airflow (Docker Compose, local)              fases 1-2b         │
│                                                                   │
│  resolve_url → download_zip → validate_download                  │
│                                      │                            │
│                                      ▼                            │
│                             stage_productos                       │
│                        (desanida, valida, Parquet)                │
│                                 │        │                        │
│                    load_bigquery   load_bigquery_comercio          │
└─────────────────────┼────────────────────┼───────────────────────┘
                       ▼                    ▼
        ┌───────────────────────────────────────────┐
        │  BigQuery: sepa.productos, sepa.comercio     │  fase 2b
        │  particionadas por dt, expiración 3 días      │  fase 4b
        └───────────────────┬───────────────────────────┘
                             ▼
        ┌───────────────────────────────────────────┐
        │  dbt (dataset sepa_dbt)                      │  fase 3
        │  stg_productos, stg_comercios                 │
        │        │                                      │
        │        ▼                                      │
        │  mart_precio_por_kilo, mart_brecha_precios     │
        │  (sin expiración)                              │
        └───────────────────────────────────────────┘

GitHub Actions (push/PR):     lint + pytest + dbt parse        fase 4a
GitHub Actions (manual):      probe-sepa (chequeo sin creds)   fase 4b
```

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

## Fase 2b, paso 2: carga a BigQuery (sandbox, sin GCS)

### Arquitectura

```
resolve_url ──▶ download_zip ──▶ validate_download ──┐
     │                                                 │ (solo orden)
     └────────────────┬────────────────────────────────▶ stage_productos
                       │ (necesita partition_date)             │
                       │                                        │ (dato real:
                       │                                        │  Parquet en disco)
                       └────────────────────────────────────────▶ load_bigquery
                                                                       │
                                                                       ▼
                                                   pipeline.bigquery_load
                                                   .load_productos_partition()
                                                                       │
                                               1 load job por archivo Parquet
                                               (primero WRITE_TRUNCATE, resto
                                                WRITE_APPEND, sin pasar por GCS)
                                                                       │
                                                                       ▼
                                      BigQuery: sepa.productos$20261119
                                      (particionada por dt, clusterizada
                                       por id_comercio)
```

A diferencia de `stage_productos` (que solo necesita el `partition_date`
de `resolve_url`), `load_bigquery` sí usa el resultado real de
`stage_productos` como input: necesita que los Parquet ya estén escritos
en disco antes de leerlos, así que la dependencia es de datos, no
forzada con `>>`.

### Decisiones de diseño

- **Sin GCS, `load_table_from_file` directo.** El sandbox de BigQuery
  (proyecto sin facturación habilitada) permite load jobs igual que
  cualquier proyecto - lo que no permite son inserts por streaming ni DML.
  Subir el Parquet local directo evita un landing intermedio en GCS que,
  para este volumen (~15M filas, archivos de pocas decenas de MB cada
  uno), no agrega nada.
- **Un load job por archivo, no uno por el staging completo.** Mismo
  criterio que el fix de OOM del paso anterior: en vez de concatenar los
  16-18 Parquet de un día en un DataFrame único y subirlo de una, cada
  archivo (ya es uno por comercio, ver fase 2a) se sube en su propio load
  job. El primero usa `WRITE_TRUNCATE` contra el decorador de partición
  (`productos$20261119`), el resto `WRITE_APPEND` - el resultado neto es
  "la partición queda con exactamente estos archivos", sin pasar nunca por
  un DataFrame de 15M filas en memoria.
- **Idempotencia por partición, no por tabla.** El `WRITE_TRUNCATE` actúa
  sobre el decorador `$YYYYMMDD`, no sobre la tabla - recargar el mismo
  día reemplaza solo esa partición. Verificado con un retry real de
  Airflow (`airflow tasks clear` sobre `load_bigquery`, no solo
  corriendo el script dos veces): mismo `count(*)` antes y después
  (14.954.793).
- **Credenciales: nunca en el repo.** La key de la service account vive
  en `~/.gcp/` (fuera del repo) y se monta de solo lectura vía
  `GCP_SA_KEY_PATH` (variable en `.env`, gitignorado). `docker-compose.yml`
  solo referencia la ruta de montaje *dentro* del contenedor
  (`GOOGLE_APPLICATION_CREDENTIALS=/opt/airflow/secrets/gcp-key.json`),
  nunca el valor real. `.gitignore` suma `*.json` y `secrets/` como
  defensa adicional.
- **`fecha_archivo` queda STRING en BigQuery**, no DATE: es el tipo físico
  que escribe pyarrow para un `str` de Python, y castear STRING→DATE
  dentro de un load job de Parquet no es un comportamiento garantizado -
  mejor resolverlo en SQL (fase 3, dbt) que arriesgar que el load falle a
  mitad de un día con varios archivos ya cargados.
- **La task falla sin paños tibios.** A diferencia de `stage_productos`
  (un comercio roto no tira la task), en `load_bigquery` no hay "parcial
  que tolerar": si un load job falla, `job.result()` levanta la excepción
  y se deja propagar tal cual.

### Cómo correrlo

```bash
docker compose exec airflow-scheduler airflow dags trigger sepa_ingest_daily
```

Verificación manual contra BigQuery:

```bash
docker compose exec airflow-scheduler python -c "
from google.cloud import bigquery
client = bigquery.Client()
for row in client.query('select count(*) as filas from \`sepa.productos\`').result():
    print(row.filas)
"
```

### Resultado de la corrida real (2026-11-19)

```
BigQuery 2026-11-19: 16 archivos, 14954793 filas cargadas en sepa.productos$20261119
```

- `count(*)` en BigQuery: **14.954.793** (idéntico a las filas escritas en
  `data/staging/`).
- Tabla confirmada particionada por `dt` (DAY) y clusterizada por
  `id_comercio` (metadata leída con `client.get_table`).
- Precio promedio por comercio (query de ejemplo), comercio 15 (DIA) como
  el más grande: 4.334.549 filas, precio promedio ~$5.339.
- Retry real de `load_bigquery` vía `airflow tasks clear`: mismo
  `count(*)` antes y después - no duplica.

## Fase 3: modelado con dbt

### Prerrequisito: cargar también `comercio` a BigQuery

Hasta la fase 2b solo se cargaba `productos`. `stg_comercios` necesita
leer de algún lado, así que `pipeline/bigquery_load.py` se generalizó a
`load_table_partition(staging_dir, table_id, ...)` - mismo patrón atómico
por partición (un load job por archivo, `WRITE_TRUNCATE` + `WRITE_APPEND`)
para cualquier tabla, no una función nueva por tabla. Task
`load_bigquery_comercio` agregada al DAG, en paralelo a `load_bigquery`.

### Arquitectura

```
BigQuery: sepa.productos, sepa.comercio  (cargadas por el DAG, fase 2b)
                    │
                    ▼
┌──────────────────────────────────────────────────────────┐
│  dbt (dataset sepa_dbt)                                    │
│                                                              │
│  staging/                                                   │
│    stg_productos   - tipos, fecha_archivo DATE, unidad       │
│                      normalizada, precio_por_kilo,            │
│                      es_outlier (sin borrar filas)            │
│    stg_comercios   - 1 fila por id_comercio (dedup banderas)  │
│                    │                                           │
│                    ▼                                           │
│  marts/                                                        │
│    mart_precio_por_kilo   - por producto+comercio+día          │
│    mart_brecha_precios    - ranking de brecha entre comercios  │
└──────────────────────────────────────────────────────────┘
```

### Por qué no usamos `productos_precio_referencia`

El SEPA ya trae un campo que *parece* ser "precio normalizado":
`productos_precio_referencia` + `productos_cantidad_referencia` +
`productos_unidad_medida_referencia`. Antes de construir `stg_productos`
sobre ese campo, lo verificamos contra datos reales - y resultó
inconsistente entre comercios.

Ejemplo real (comercio 10, WHISKY BLENDED SCOTCH 12 AÑOS BUCHANAN'S, botella
de 750cc):

| campo | valor |
|---|---|
| `productos_precio_lista` | $79.900 |
| `productos_cantidad_presentacion` / unidad | 1 UNI |
| `productos_cantidad_referencia` | **750** |
| `productos_unidad_medida_referencia` | CM3 |
| `productos_precio_referencia` | **$106.533,33** |

$106.533,33 = $79.900 / 750 × **1000** - el precio está normalizado "por
cada 1000cc" (por litro), pero el campo `cantidad_referencia` dice
literalmente "750", no "1000". Si se divide `precio_referencia` por
`cantidad_referencia` tal como están (para "deshacer" la normalización y
rehacerla por kilo), el resultado es incorrecto - el campo no es
consistente en qué base usa. Confirmamos el mismo patrón en varios
productos y comercios distintos: no es un error puntual de una fila, es
una inconsistencia estructural del campo entre exportadores.

**Decisión:** `stg_productos.precio_por_kilo` se calcula desde cero con
`productos_precio_lista / productos_cantidad_presentacion`, usando
nuestra propia normalización de unidad (`GR`/`KG`/`ML`/`LT`/`UN`/`OTRO`),
en vez de confiar en un campo derivado que la propia fuente calcula de
forma distinta según el comercio. `productos_precio_referencia` se
conserva en `stg_productos` solo para auditoría/debug, ningún modelo lo
usa para normalizar.

### `es_outlier`: se marca, no se borra

Una fila con `productos_precio_lista <= 0`, o que se desvía más de
`outlier_ratio_threshold` veces (default 3x, var de `dbt_project.yml`) de
la mediana de precios del mismo EAN ese mismo día, se marca
`es_outlier = true` en `stg_productos` - la fila sigue ahí, visible para
quien quiera auditarla. Ambos marts la excluyen de sus agregados
(`where not es_outlier`), pero no la eliminan de la capa de staging.

Encontrado así en datos reales: un six-pack de cerveza Heineken a
**$332.600** en un comercio, cuando la mediana de ese mismo EAN entre
comercios ese día era un orden de magnitud menor - claramente un error de
carga de precio de ese comercio puntual, no un precio real.

### `mart_brecha_precios`: filtros para que el ranking tenga sentido

- **Misma fecha real, no solo mismo `dt`.** Se exige que `fecha_archivo`
  esté a lo sumo `max_lag_days_brecha` días (default 1, var de
  `dbt_project.yml`) del `dt` de la partición - comparar el precio de hoy
  de un comercio contra el de la semana pasada de otro no es una brecha
  real, es ruido de frescura de datos.
- **Mínimo de comercios configurable** (`min_comercios_brecha`, default
  3): con 2 comercios nomás, "ranking de brecha" es solo "cuál de los dos
  es más caro", no dice mucho.
- Ambos son `vars` de dbt, no números sueltos en el SQL - se pueden
  ajustar sin tocar el modelo.

### Tests y documentación

27 tests (`not_null`, `unique` sobre una clave surrogate por modelo,
`accepted_values` sobre la unidad normalizada) corriendo juntos contra
BigQuery real - `dbt test --profiles-dir .` desde `dbt/radar_precios/`.

Un hallazgo real de los tests: el hash de `(dt, id_comercio, id_sucursal,
id_producto)` no alcanzaba como clave de `stg_productos` - el comercio 6
reutiliza el mismo rango de `id_sucursal` entre sus 2 banderas, así que
hubo que sumar `id_bandera`. Incluso así, 49 filas de un comercio distinto
(comercio 5) quedaban duplicadas *exactas* (mismo precio, misma
descripción, todos los campos iguales) - un bug real del export de ese
comercio, resuelto con un `QUALIFY ROW_NUMBER()` en vez de ignorar el test.

```bash
cd dbt/radar_precios
export GCP_PROJECT_ID=radar-precios-515302
export GOOGLE_APPLICATION_CREDENTIALS=~/.gcp/radar-precios-key.json

dbt run --profiles-dir .
dbt test --profiles-dir .
dbt docs generate --profiles-dir .
dbt docs serve --profiles-dir .   # sirve target/index.html en localhost
```

### Resultado real

| modelo | filas | tests |
|---|---|---|
| `stg_productos` | 14.954.744 | 8/8 |
| `stg_comercios` | 16 | 5/5 |
| `mart_precio_por_kilo` | 39.800 | 6/6 |
| `mart_brecha_precios` | 11.600 | 8/8 |

Top del ranking de brecha real: un bombón/chocolate con 361% de diferencia
entre el comercio más barato y el más caro, con 3 comercios comparables -
exactamente el mínimo configurado, ninguno de menos.

## Fase 4a: CI con GitHub Actions

### Workflow

`.github/workflows/ci.yml` corre 3 jobs en paralelo en cada push y pull
request:

- **lint**: `ruff check .` sobre todo el repo (`pyproject.toml`: reglas
  E/F/I/UP, line-length 110).
- **test**: `pytest tests/` - los tests de `pipeline.bigquery_load` ya
  corren con el cliente de BigQuery mockeado (`unittest.mock`), así que
  toda la suite pasa sin ninguna credencial de GCP.
- **dbt-parse**: `dbt parse` sobre `dbt/radar_precios`, con
  `GCP_PROJECT_ID` en un valor cualquiera y **sin**
  `GOOGLE_APPLICATION_CREDENTIALS` seteada - `dbt parse` solo valida
  Jinja/YAML/referencias entre modelos, nunca abre una conexión contra
  BigQuery. Confirmado corriéndolo a mano sin ninguna credencial antes de
  escribir el workflow.

Ningún secreto de GCP llegó a GitHub todavía (ni a Secrets del repo ni al
workflow) - eso queda para la fase 4b, cuando el CI necesite correr algo
contra BigQuery real (`dbt test`, por ejemplo).

Un hallazgo real al primer push: `pytest tests/ -q` (el binario directo)
no agrega el repo al `sys.path`, a diferencia de `python -m pytest` (que
sí lo hace) - localmente nunca se notó porque siempre se corrió con `-m`.
En CI explotó con `ModuleNotFoundError: No module named 'pipeline'` en los
7 módulos de test. Fix de raíz en `pyproject.toml`
(`[tool.pytest.ini_options] pythonpath = ["."]`) en vez de parchear el
comando del workflow, para que funcione sin importar cómo se invoque
pytest.

### Retención de datos: medición real y propuesta

`sepa.productos` (la tabla cruda, partición única de hoy) pesa:

| Tabla | Filas | Tamaño |
|---|---|---|
| `sepa.productos` | 14.954.793 | **1,97 GB** |
| `sepa.comercio` | 29 | ~0 MB |
| `sepa_dbt.mart_precio_por_kilo` | 39.769 | 6,2 MB |
| `sepa_dbt.mart_brecha_precios` | 11.631 | 1,66 MB |

Si se agrega una partición diaria de ese tamaño a `sepa.productos` sin
ninguna política de retención, a **~1,97 GB/día** la capa gratuita de
BigQuery (10 GB de storage) se agota en **~5 días**. Los marts, en
cambio, pesan entre 500 y 4.000 veces menos - acumular meses de historia
ahí es prácticamente gratis.

**Propuesta en base a esta medición** (en este punto todavía no aplicada
al código - la implementación y la verificación contra BigQuery real
están en [fase 4b, paso 1](#fase-4b-paso-1-política-de-retención-en-bigquery_loadpy)):

- `sepa.productos` y `sepa.comercio` (crudo): `time_partitioning.
  expiration_ms` de **3 días**. Es barato de regenerar si hiciera falta -
  el pipeline entero (fase 1-2b) es idempotente desde el ZIP original, así
  que no hay razón para pagar storage por el crudo histórico más allá de
  una ventana corta de debugging/reprocesamiento.
- Marts (`mart_precio_por_kilo`, `mart_brecha_precios`): **sin
  expiración** - son los que valen para análisis histórico, y a este
  ritmo (~8 MB/día combinados) tardarían años en pesar lo que el crudo
  pesa en una semana.

## Fase 4b, paso 1: política de retención en `bigquery_load.py`

Implementa la propuesta de la sección anterior: `ensure_table()` acepta
`partition_expiration_days` (default 3, `None` = sin expiración).

- Si la tabla no existe, se crea con la expiración ya puesta.
- Si ya existe, **nunca se recrea** (perdería las particiones ya
  cargadas, p.ej. los datos de hoy) - se le actualiza la política vía
  `update_table(["time_partitioning"])` solo si cambió; si ya coincide,
  no hace ninguna llamada de más.
- Los marts de dbt no pasan por este módulo - no hay ninguna entrada para
  ellos, así que "sin expiración" ya es el comportamiento por default sin
  código especial.

14 tests con el cliente de BigQuery mockeado (creación con expiración
default/custom/`None`, actualización sin recrear, no-op si ya coincide).

Verificado contra BigQuery real, no solo contra el código - se leyó la
metadata antes y después de aplicar:

| Tabla | `expiration_ms` antes | `expiration_ms` después | Filas |
|---|---|---|---|
| `sepa.productos` | `None` | **259200000** (3,0 días) | 14.954.793 (sin cambios) |
| `sepa.comercio` | `None` | **259200000** (3,0 días) | 29 (sin cambios) |

Corrido dos veces: la primera aplicó el cambio, la segunda confirmó que
es no-op (ninguna llamada de más a la API).

## Fase 4b, paso 2: prueba de acceso al SEPA sin credenciales

`pipeline/sepa_source.py` extrae la resolución de la URL (el dict
`DAY_RESOURCES` y el armado de la URL) que antes vivía duplicada dentro
del DAG, para que tanto Airflow como una herramienta standalone
(`scripts/probe_sepa.py`) usen la misma única fuente de verdad.

`scripts/probe_sepa.py` pide el primer MB del ZIP del SEPA con header
`Range`, pero además **streamea y corta del lado del cliente** al llegar
a ese límite - así que aunque el servidor ignore el `Range` y mande el
archivo completo con status 200 (que es justo lo que pasó en la prueba
local, ver abajo), nunca se descarga más de ~1MB.

`.github/workflows/probe-sepa.yml`: `workflow_dispatch` únicamente (nunca
en push/PR), sin secrets ni variables de GCP - es un GET HTTP a un
endpoint público.

**Resultado real**, corriendo el mismo script en los dos entornos:

| Origen | HTTP status | `Content-Range` | Bytes recibidos |
|---|---|---|---|
| Local | 200 | `bytes 0-329502961/329502962` (tamaño real) | ~1 MB (tope del cliente) |
| Runner de GitHub Actions (`gh workflow run probe-sepa.yml`) | **403** | `None` | 69.286 (página de error) |

Ver "Decisiones de arquitectura" arriba para la interpretación completa:
el SEPA bloquea la IP del runner, no es un problema del workflow ni del
código - el script resolvió la URL bien y completó la conexión en los dos
casos.

## Fase 4b, paso 3: procesar el comercio más grande por partes

### El problema

`stage_productos` moría con `OOMKilled: true` corriendo dentro del
contenedor de Airflow, de forma reproducible. Antes de tocar código se
midió con precisión (`docker stats`, muestreo cada 1-3 segundos, dos
corridas limpias): **pico de 5,77 GiB y 6,03 GiB**, contra un techo real
de **7,75 GiB** (la VM de Docker Desktop entera, no los 12GB que se creía
tener configurados - revisar Settings → Resources).

La causa: `pipeline/parse.py::parse_productos_csv` leía el `productos.csv`
del comercio más grande (comercio 15, DIA: **510 MB sin comprimir**,
~4,3M filas) completo a un solo DataFrame de pandas, y
`apply_row_level_price_rules` sumaba 2-3 copias completas de ese tamaño
en el camino (`df.copy()`, `df[~mask].copy()`).

### El cambio

`pipeline/parse.py::parse_productos_csv_chunks()` (generador) +
`pipeline/stage.py::_process_comercio` ahora procesan productos.csv de a
`chunk_size_rows` filas (default 300.000, **configurable** por parámetro
de `run_staging()`/`_process_comercio()` y como `Param` del DAG). Cada
chunk se valida y se escribe a su propio Parquet apenas se procesa, en un
directorio de scratch por comercio - la decisión de cuarentenar por
`bad_ratio` solo puede tomarse después de ver todos los chunks, así que
esos archivos se "confirman" (se mueven al destino final) o se borran
enteros según el resultado, sin dejar rastro en ningún caso.

`validate_structural()` se separó en `pipeline.validate` en
`validate_comercio_tables()` (comercio.csv/sucursales.csv/fecha_archivo,
sin necesitar productos_df) + el chequeo de productos vacío - para poder
descartar un comercio por comercio.csv vacío sin leer productos.csv del
comercio más grande primero.

### Medición de memoria: antes y después

| | Pico medido | Método |
|---|---|---|
| **Antes** (corrida completa, 16 comercios) | 5,77 GiB / 6,03 GiB | `docker stats`, dos corridas |
| **Antes** (comercio 15 aislado) | **OOM-killed** (ni siquiera termina) | `resource.getrusage` |
| **Después** (corrida completa, 16 comercios) | **5,47-5,62 GiB** | `docker stats`, dos corridas |
| **Después** (comercio 15 aislado) | **4,92 GiB** (delta +3,6 GiB sobre una base de 1,3 GiB) | `resource.getrusage`, medición precisa del proceso |

**Resultado honesto, no inflado:** la reducción del pico en la corrida
completa es modesta (~5-10%), no dramática. La prueba más contundente no
es el número sino la confiabilidad: el método viejo, corrido en
aislamiento sobre *solo* el comercio más grande, **ni siquiera llega a
terminar** (muere por OOM antes de poder medir nada) - el método nuevo sí
termina, de forma reproducible, en 3 corridas completas consecutivas sin
un solo OOM.

**Por qué el pico sigue siendo alto:** el chunking está en la etapa de
pandas (`pd.read_csv(chunksize=...)`), pero `_decode()` y
`_strip_footer()` todavía procesan el texto completo del archivo de una
sola vez - para el comercio más grande, eso son ~510MB de bytes crudos +
~510MB de texto decodificado + ~510MB de texto sin el footer, coexistiendo
antes de que pandas vea el primer chunk. Esa es la próxima capa a
chunkear si hiciera falta bajar más el pico (leer y decodificar el ZIP en
streaming, sin materializar el texto completo) - **no implementado en
este paso**, el pedido era específicamente sobre el procesamiento por
partes a nivel DataFrame/fila, que es lo que se construyó y lo que
resolvió el OOM reproducible.

### Verificación de equivalencia e idempotencia

- **Equivalencia**: test de `pipeline/parse.py` que compara
  `parse_productos_csv()` (método viejo) contra concatenar
  `parse_productos_csv_chunks()` (método nuevo, forzando el corte en
  medio de los datos) - mismo DataFrame exacto. Test de integración en
  `pipeline/stage.py` que corre `run_staging()` completo con
  `chunk_size_rows=10_000` vs `=7` sobre el mismo comercio sintético
  (mezcla de filas sanas/descartadas/corregidas) - mismos conteos, mismo
  Parquet final. Confirmado también contra el ZIP real completo:
  `chunk_size_rows=300_000` vs `=10_000_000` (~1 chunk) dan el
  **mismo DataFrame de 14.954.793 filas**, comparado con
  `pd.testing.assert_frame_equal`.
- **Idempotencia**: corrida real dos veces dentro del contenedor, mismo
  `chunk_size_rows` - MD5 combinado de todos los archivos de salida
  idéntico. Ya no hace falta un `.scratch-*` vacío colgando en la
  partición final (test dedicado para esto).

### Qué falta (fase 4b en adelante)

Agregar las credenciales de GCP como Secret de GitHub para que el CI
pueda correr `dbt test` contra BigQuery real, y más adelante un servidor
MCP sobre los marts para consultar precios desde Claude.
