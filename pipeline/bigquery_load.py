"""Carga el staging de productos (fase 2a/2b-paso1) a BigQuery, sin GCS de
por medio: `load_table_from_file` sube el Parquet local directo.

Idempotencia por partición, no por tabla. `productos` está particionada
por la columna `dt` (la fecha de descarga del ZIP, fase 1 - no
`fecha_archivo`, que es por comercio) y clusterizada por `id_comercio`.
Para recargar un día sin duplicar filas, el destino de cada load job es el
decorador de partición `productos$YYYYMMDD` en vez de `productos` a secas:
BigQuery reemplaza solo esa partición, nunca la tabla completa.

Un día real tiene ~15-29M filas repartidas en ~16-18 archivos Parquet (uno
por comercio, ver pipeline.stage). Para no repetir el error de memoria de
la fase 2b-paso1 (concatenar todo antes de escribir), cada archivo se sube
en su propio load job, streameado desde disco - nunca se junta todo en un
DataFrame. El primer archivo usa WRITE_TRUNCATE (limpia la partición), el
resto WRITE_APPEND (se suma a lo que acaba de limpiar el primero) - el
resultado neto es "la partición queda con exactamente estos archivos", que
es lo mismo que pisarla entera de una, pero sin el pico de memoria de
armar un solo archivo gigante.
"""
from __future__ import annotations

import io
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pandas as pd
from google.cloud import bigquery

DATASET_ID = "sepa"
TABLE_ID = "productos"

# dtype real de productos_df (pipeline.parse/pipeline.stage) -> tipo de
# BigQuery. fecha_archivo queda STRING (no DATE): es el tipo físico que
# pyarrow escribe para un str de Python, y una conversión STRING->DATE en
# un load job de Parquet no es segura - mejor castearla en SQL más
# adelante (fase 3, dbt) que arriesgar que el load job falle.
SCHEMA = [
    bigquery.SchemaField("id_comercio", "STRING"),
    bigquery.SchemaField("id_bandera", "STRING"),
    bigquery.SchemaField("id_sucursal", "STRING"),
    bigquery.SchemaField("id_producto", "STRING"),
    bigquery.SchemaField("productos_ean", "STRING"),
    bigquery.SchemaField("productos_descripcion", "STRING"),
    bigquery.SchemaField("productos_cantidad_presentacion", "STRING"),
    bigquery.SchemaField("productos_unidad_medida_presentacion", "STRING"),
    bigquery.SchemaField("productos_marca", "STRING"),
    bigquery.SchemaField("productos_precio_lista", "FLOAT64"),
    bigquery.SchemaField("productos_precio_referencia", "FLOAT64"),
    bigquery.SchemaField("productos_cantidad_referencia", "STRING"),
    bigquery.SchemaField("productos_unidad_medida_referencia", "STRING"),
    bigquery.SchemaField("productos_precio_unitario_promo1", "FLOAT64"),
    bigquery.SchemaField("productos_leyenda_promo1", "STRING"),
    bigquery.SchemaField("productos_precio_unitario_promo2", "FLOAT64"),
    bigquery.SchemaField("productos_leyenda_promo2", "STRING"),
    bigquery.SchemaField("fecha_archivo", "STRING"),
    bigquery.SchemaField("dt", "DATE"),  # columna de partición
]


@dataclass
class LoadResult:
    partition_date: date
    files_loaded: int
    rows_loaded: int


def ensure_dataset(client: bigquery.Client, dataset_id: str, location: str) -> None:
    dataset_ref = bigquery.DatasetReference(client.project, dataset_id)
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = location
    client.create_dataset(dataset, exists_ok=True)


def ensure_table(client: bigquery.Client, dataset_id: str, table_id: str) -> None:
    table_ref = bigquery.DatasetReference(client.project, dataset_id).table(table_id)
    table = bigquery.Table(table_ref, schema=SCHEMA)
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY, field="dt"
    )
    table.clustering_fields = ["id_comercio"]
    client.create_table(table, exists_ok=True)


def _iter_part_files(staging_dir: Path, partition_date: date) -> list[Path]:
    partition_dir = staging_dir / "productos" / f"dt={partition_date.isoformat()}"
    return sorted(p for p in partition_dir.glob("*.parquet") if p.stat().st_size > 0)


def load_productos_partition(
    staging_dir: Path,
    partition_date: date,
    project_id: str,
    location: str = "US",
    client: bigquery.Client | None = None,
) -> LoadResult:
    client = client or bigquery.Client(project=project_id)

    ensure_dataset(client, DATASET_ID, location)
    ensure_table(client, DATASET_ID, TABLE_ID)

    part_files = _iter_part_files(staging_dir, partition_date)
    if not part_files:
        raise ValueError(
            f"No encontré ningún Parquet de productos para {partition_date} en {staging_dir} "
            "- stage_productos tendría que haber fallado antes de llegar acá."
        )

    destination = (
        f"{client.project}.{DATASET_ID}.{TABLE_ID}${partition_date.strftime('%Y%m%d')}"
    )
    rows_loaded = 0

    for i, part_file in enumerate(part_files):
        df = pd.read_parquet(part_file)
        if df.empty:
            continue
        df["dt"] = partition_date

        buf = io.BytesIO()
        df.to_parquet(buf, index=False)
        buf.seek(0)

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            schema=SCHEMA,
            write_disposition=(
                bigquery.WriteDisposition.WRITE_TRUNCATE
                if i == 0
                else bigquery.WriteDisposition.WRITE_APPEND
            ),
        )
        job = client.load_table_from_file(buf, destination, job_config=job_config)
        job.result()  # espera a que termine el load job y levanta si falló
        rows_loaded += len(df)

    return LoadResult(partition_date=partition_date, files_loaded=len(part_files), rows_loaded=rows_loaded)
