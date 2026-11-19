"""
Descarga diaria del dump de precios del SEPA (datos.produccion.gob.ar).

La fuente no publica un archivo "del 19/11" sino un archivo por día de la
semana (sepa_lunes.zip ... sepa_domingo.zip) que el Estado sobrescribe todos
los días con los datos del día. No hay catálogo histórico detrás de esta URL:
si no lo guardamos hoy, el dato de hoy se pierde en cuanto el Estado publique
el próximo lunes. Por eso:

  - catchup=False y sin backfill: no existe "el archivo del lunes pasado" para
    recuperar, así que pedírselo a Airflow sería pedirle algo que no se puede
    cumplir.
  - Usamos la hora de reloj (pendulum.now) en vez del logical_date/ds de
    Airflow para decidir qué día de la semana pedir. El logical_date de un
    DAG con schedule cron representa el *inicio* del intervalo de datos, no
    necesariamente "hoy"; como la fuente solo expone el snapshot de hoy (no un
    intervalo), guiarse por el logical_date llevaría a pedir el archivo
    equivocado en el primer run después de una falla o un redeploy.
"""
from __future__ import annotations

import logging
import os
import zipfile
from datetime import date
from pathlib import Path

import pendulum
import requests
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.models.param import Param
from airflow.operators.python import get_current_context

from pipeline.stage import DEFAULT_MAX_BAD_ROW_RATIO, find_daily_zip, run_staging
from pipeline.validate import DEFAULT_MAX_LAG_DAYS

log = logging.getLogger(__name__)

TIMEZONE = "America/Argentina/Buenos_Aires"

DATASET_ID = "6f47ec76-d1ce-4e34-a7e1-621fe9b1d0b5"

# weekday() de Python: Monday=0 ... Sunday=6.
# IDs de recurso tomados de datos.produccion.gob.ar/dataset/sepa-precios.
# Si en el futuro empiezan a devolver 404, lo primero a revisar es si el
# dataset fue reestructurado y estos IDs de recurso cambiaron.
DAY_RESOURCES = {
    0: ("lunes", "0a9069a9-06e8-4f98-874d-da5578693290"),
    1: ("martes", "9dc06241-cc83-44f4-8e25-c9b1636b8bc8"),
    2: ("miercoles", "1e92cd42-4f94-4071-a165-62c4cb2ce23c"),
    3: ("jueves", "d076720f-a7f0-4af8-b1d6-1b99d5a90c14"),
    4: ("viernes", "91bc072a-4726-44a1-85ec-4a8467aad27e"),
    5: ("sabado", "b3c3da5d-213d-41e7-8d74-f23fda0a3c30"),
    6: ("domingo", "f8e75128-515a-436e-bf8d-5c63a62f2005"),
}

RAW_DIR = Path("/opt/airflow/data/raw")
RAW_DATA_DIR = RAW_DIR / "sepa"
STAGING_DIR = Path("/opt/airflow/data/staging")

# Un zip de un día real pesa ~300MB. Si bajamos menos de esto, lo más probable
# es que el servidor haya devuelto una página de error en vez del archivo.
MIN_EXPECTED_BYTES = 5_000_000


@dag(
    dag_id="sepa_ingest_daily",
    description="Descarga el ZIP diario de precios del SEPA a la zona raw.",
    schedule="0 18 * * *",  # 18:00 Argentina: el Estado ya actualizó el archivo del día.
    start_date=pendulum.datetime(2026, 11, 18, tz=TIMEZONE),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "juan",
        "retries": 3,
        "retry_delay": pendulum.duration(minutes=5),
    },
    # Configurables desde la UI de Airflow al disparar el run ("Trigger DAG
    # w/ config"), con los mismos defaults que pipeline/ (fase 2a) - una
    # sola fuente de verdad para los umbrales, no un número copiado a mano.
    params={
        "max_bad_row_ratio": Param(
            DEFAULT_MAX_BAD_ROW_RATIO,
            type="number",
            minimum=0,
            maximum=1,
            description="Proporción máxima de filas descartadas+corregidas antes de cuarentenar un comercio entero.",
        ),
        "max_lag_days": Param(
            DEFAULT_MAX_LAG_DAYS,
            type="integer",
            minimum=0,
            description="Días de atraso en fecha_archivo tolerados antes de cuarentenar un comercio.",
        ),
    },
    tags=["sepa", "ingest"],
)
def sepa_ingest_daily():

    @task
    def resolve_url() -> dict:
        today = pendulum.now(TIMEZONE)
        day_name, resource_id = DAY_RESOURCES[today.weekday()]
        url = (
            f"https://datos.produccion.gob.ar/dataset/{DATASET_ID}"
            f"/resource/{resource_id}/download/sepa_{day_name}.zip"
        )
        return {
            "url": url,
            "day_name": day_name,
            "partition_date": today.to_date_string(),  # YYYY-MM-DD
        }

    @task
    def download_zip(resolved: dict) -> dict:
        partition_dir = RAW_DATA_DIR / f"dt={resolved['partition_date']}"
        partition_dir.mkdir(parents=True, exist_ok=True)
        dest_path = partition_dir / f"sepa_{resolved['day_name']}.zip"
        # Se escribe a un .part en el mismo directorio y se renombra al final.
        # os.replace() es atómico dentro del mismo filesystem: evita que un
        # retry, un re-run manual o una corrida concurrente deje el archivo
        # final a medio escribir mientras otra tarea (o un run previo) todavía
        # lo está leyendo para validarlo. Pasó en una prueba manual: un
        # trigger manual quedó encolado por max_active_runs=1 y arrancó justo
        # cuando el run anterior ya había terminado, pisando el archivo final
        # mientras lo verificábamos a mano.
        tmp_path = dest_path.with_suffix(dest_path.suffix + ".part")

        with requests.get(resolved["url"], stream=True, timeout=(10, 300)) as response:
            response.raise_for_status()
            with open(tmp_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    f.write(chunk)

        os.replace(tmp_path, dest_path)
        return {"path": str(dest_path), "size_bytes": os.path.getsize(dest_path)}

    @task
    def validate_download(download_info: dict) -> None:
        path = download_info["path"]
        size_bytes = download_info["size_bytes"]

        if size_bytes < MIN_EXPECTED_BYTES:
            raise AirflowException(
                f"{path} pesa {size_bytes} bytes, menos de los "
                f"{MIN_EXPECTED_BYTES} esperados: probablemente no sea el "
                "ZIP real sino una página de error."
            )
        if not zipfile.is_zipfile(path):
            raise AirflowException(f"{path} no es un ZIP válido.")

    @task
    def stage_productos(resolved: dict) -> dict:
        """Corre pipeline.stage.run_staging (fase 2a) sobre el ZIP que ya
        bajó y validó download_zip/validate_download. Su input real es
        resolved (necesita partition_date, no el resultado de
        validate_download) - la dependencia de orden con validate_download
        se fuerza más abajo con `>>`, no pasándole el dato.

        Un comercio roto o fuera de los umbrales configurados NO hace
        fallar esta task (pipeline.stage ya lo manda a cuarentena/inactivos
        y sigue con el resto) - el resumen queda en el log. Si ningún
        comercio pasó, eso sí es una falla real de la task."""
        context = get_current_context()
        max_bad_row_ratio = context["params"]["max_bad_row_ratio"]
        max_lag_days = context["params"]["max_lag_days"]

        partition_date = date.fromisoformat(resolved["partition_date"])
        daily_zip_path = find_daily_zip(RAW_DIR, partition_date)

        result = run_staging(
            daily_zip_path,
            partition_date,
            STAGING_DIR,
            max_bad_row_ratio=max_bad_row_ratio,
            max_lag_days=max_lag_days,
        )

        total = len(result.ok_comercio_ids) + len(result.quarantined) + len(result.inactive_comercios)
        log.info(
            "staging %s: %d/%d comercios ok, %d en cuarentena, %d inactivos, filas escritas=%s",
            partition_date,
            len(result.ok_comercio_ids),
            total,
            len(result.quarantined),
            len(result.inactive_comercios),
            result.rows_written,
        )
        for q in result.quarantined:
            log.warning("comercio %s en cuarentena: %s", q.comercio_id, q.reason)
        for c in result.inactive_comercios:
            log.info(
                "comercio %s inactivo hace %d días (sin actualizar desde %s) - no se repite en "
                "la cuarentena diaria, ver _inactive_comercios/",
                c.comercio_id,
                c.lag_days,
                c.fecha_archivo,
            )

        if not result.ok_comercio_ids:
            raise AirflowException(
                f"Ningún comercio pasó el staging de {partition_date} "
                f"({len(result.quarantined)} en cuarentena, {len(result.inactive_comercios)} inactivos) - "
                "esto es más grave que la falla puntual de un comercio, probablemente el ZIP del día "
                "esté mal."
            )

        return {
            "ok_comercios": len(result.ok_comercio_ids),
            "quarantined_comercios": len(result.quarantined),
            "inactive_comercios": len(result.inactive_comercios),
            "rows_written": result.rows_written,
        }

    resolved = resolve_url()
    validated = validate_download(download_zip(resolved))
    staged = stage_productos(resolved)
    validated >> staged


sepa_ingest_daily()
