from datetime import date
from unittest.mock import MagicMock

import pandas as pd
import pytest
from google.cloud import bigquery

from pipeline.bigquery_load import (
    LoadResult,
    ensure_dataset,
    ensure_table,
    load_comercio_partition,
    load_productos_partition,
)

PARTITION_DATE = date(2026, 11, 19)


def _write_part(staging_dir, comercio_id: int, n_rows: int = 2, table_id: str = "productos"):
    partition_dir = staging_dir / table_id / f"dt={PARTITION_DATE.isoformat()}"
    partition_dir.mkdir(parents=True, exist_ok=True)
    if table_id == "productos":
        df = pd.DataFrame(
            {
                "id_comercio": [str(comercio_id)] * n_rows,
                "productos_precio_lista": [100.0] * n_rows,
            }
        )
    else:
        df = pd.DataFrame(
            {
                "id_comercio": [str(comercio_id)] * n_rows,
                "comercio_razon_social": [f"Comercio {comercio_id} S.A."] * n_rows,
            }
        )
    df.to_parquet(partition_dir / f"part-{comercio_id:05d}.parquet", index=False)


def _mock_client():
    client = MagicMock(spec=bigquery.Client)
    client.project = "radar-precios-515302"
    job = MagicMock()
    client.load_table_from_file.return_value = job
    return client, job


def test_ensure_dataset_creates_with_location():
    client = MagicMock(spec=bigquery.Client)
    client.project = "proj"

    ensure_dataset(client, "sepa", "US")

    created_dataset = client.create_dataset.call_args.args[0]
    assert created_dataset.location == "US"
    assert client.create_dataset.call_args.kwargs["exists_ok"] is True


def test_ensure_table_sets_partitioning_and_clustering():
    client = MagicMock(spec=bigquery.Client)
    client.project = "proj"

    ensure_table(client, "sepa", "productos")

    created_table = client.create_table.call_args.args[0]
    assert created_table.time_partitioning.field == "dt"
    assert created_table.time_partitioning.type_ == bigquery.TimePartitioningType.DAY
    assert created_table.clustering_fields == ["id_comercio"]
    assert client.create_table.call_args.kwargs["exists_ok"] is True


def test_load_productos_partition_truncates_first_and_appends_rest(tmp_path):
    staging_dir = tmp_path / "staging"
    _write_part(staging_dir, comercio_id=1)
    _write_part(staging_dir, comercio_id=2)
    _write_part(staging_dir, comercio_id=3)
    client, job = _mock_client()

    result = load_productos_partition(
        staging_dir, PARTITION_DATE, "radar-precios-515302", client=client
    )

    assert result == LoadResult(
        table_id="productos", partition_date=PARTITION_DATE, files_loaded=3, rows_loaded=6
    )
    assert client.load_table_from_file.call_count == 3

    dispositions = [
        kwargs["job_config"].write_disposition
        for _, kwargs in client.load_table_from_file.call_args_list
    ]
    assert dispositions == [
        bigquery.WriteDisposition.WRITE_TRUNCATE,
        bigquery.WriteDisposition.WRITE_APPEND,
        bigquery.WriteDisposition.WRITE_APPEND,
    ]

    destinations = [c.args[1] for c in client.load_table_from_file.call_args_list]
    assert all(d == "radar-precios-515302.sepa.productos$20261119" for d in destinations)
    assert job.result.call_count == 3  # se esperó cada load job


def test_load_productos_partition_raises_if_no_part_files(tmp_path):
    staging_dir = tmp_path / "staging"
    (staging_dir / "productos" / f"dt={PARTITION_DATE.isoformat()}").mkdir(parents=True)
    client, _ = _mock_client()

    with pytest.raises(ValueError, match="No encontré ningún Parquet"):
        load_productos_partition(staging_dir, PARTITION_DATE, "radar-precios-515302", client=client)


def test_load_productos_partition_skips_zero_byte_files(tmp_path):
    staging_dir = tmp_path / "staging"
    _write_part(staging_dir, comercio_id=1)
    partition_dir = staging_dir / "productos" / f"dt={PARTITION_DATE.isoformat()}"
    (partition_dir / "part-00000.parquet").touch()  # placeholder vacío (ver pipeline.stage)
    client, _ = _mock_client()

    result = load_productos_partition(
        staging_dir, PARTITION_DATE, "radar-precios-515302", client=client
    )

    assert result.files_loaded == 1
    assert client.load_table_from_file.call_count == 1


def test_load_productos_partition_adds_dt_column(tmp_path):
    staging_dir = tmp_path / "staging"
    _write_part(staging_dir, comercio_id=1, n_rows=3)
    client, _ = _mock_client()

    load_productos_partition(staging_dir, PARTITION_DATE, "radar-precios-515302", client=client)

    uploaded_buf = client.load_table_from_file.call_args_list[0].args[0]
    uploaded_buf.seek(0)
    df = pd.read_parquet(uploaded_buf)
    assert (df["dt"] == PARTITION_DATE).all()


# --- tabla comercio (misma lógica genérica, distinto schema) -------------


def test_ensure_table_comercio_schema():
    client = MagicMock(spec=bigquery.Client)
    client.project = "proj"

    ensure_table(client, "sepa", "comercio")

    created_table = client.create_table.call_args.args[0]
    field_names = [f.name for f in created_table.schema]
    assert "comercio_razon_social" in field_names
    assert "productos_precio_lista" not in field_names
    assert created_table.time_partitioning.field == "dt"
    assert created_table.clustering_fields == ["id_comercio"]


def test_load_comercio_partition_truncates_first_and_appends_rest(tmp_path):
    staging_dir = tmp_path / "staging"
    _write_part(staging_dir, comercio_id=1, table_id="comercio")
    _write_part(staging_dir, comercio_id=2, table_id="comercio")
    client, job = _mock_client()

    result = load_comercio_partition(staging_dir, PARTITION_DATE, "radar-precios-515302", client=client)

    assert result == LoadResult(
        table_id="comercio", partition_date=PARTITION_DATE, files_loaded=2, rows_loaded=4
    )
    destinations = [c.args[1] for c in client.load_table_from_file.call_args_list]
    assert all(d == "radar-precios-515302.sepa.comercio$20261119" for d in destinations)
    dispositions = [
        kwargs["job_config"].write_disposition
        for _, kwargs in client.load_table_from_file.call_args_list
    ]
    assert dispositions == [bigquery.WriteDisposition.WRITE_TRUNCATE, bigquery.WriteDisposition.WRITE_APPEND]


def test_load_comercio_partition_does_not_touch_productos_files(tmp_path):
    staging_dir = tmp_path / "staging"
    _write_part(staging_dir, comercio_id=1, table_id="productos")
    _write_part(staging_dir, comercio_id=1, table_id="comercio")
    client, _ = _mock_client()

    result = load_comercio_partition(staging_dir, PARTITION_DATE, "radar-precios-515302", client=client)

    assert result.files_loaded == 1  # solo el de comercio, no el de productos
