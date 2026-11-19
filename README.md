# Radar de Precios — Pipeline

Pipeline de datos de producción sobre los datos públicos del SEPA (Sistema
Electrónico de Publicidad de Precios Argentinos), con foco en prácticas de
Data Engineering: orquestación, modelado, CI/CD y exposición de datos a un
agente (Claude vía MCP).

Dashboard relacionado: [radar-precios-ar](https://github.com/jgarciarios/radar-precios-ar).

## Roadmap

1. **Orquestación (actual)** — Airflow en Docker Compose, DAG que descarga el
   dump diario del SEPA a una zona *raw*.
2. **Staging + carga** — parseo de los CSV del ZIP y carga a BigQuery (capa
   gratuita), vía GCS como landing intermedio.
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

### Qué falta (fase 2 en adelante)

Este ZIP contiene varios CSV (comercios, sucursales, productos). La fase 1
se detiene en "tenerlo descargado y validado"; parsearlo y cargarlo a
BigQuery es la fase 2.
