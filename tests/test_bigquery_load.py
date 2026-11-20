from datetime import date
from unittest.mock import MagicMock

import pandas as pd
import pytest
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from pipeline.bigquery_load import (
    DEFAULT_PARTITION_EXPIRATION_DAYS,
    LoadResult,
    ensure_dataset,
    ensure_table,
    load_comercio_partition,
    load_productos_partition,
)

THREE_DAYS_MS = 3 * 24 * 60 * 60 * 1000

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


def _client_without_table():
    """Simula un dataset/tabla que todavía no existe: get_table levanta
    NotFound, como hace la API real."""
    client = MagicMock(spec=bigquery.Client)
    client.project = "proj"
    client.get_table.side_effect = NotFound("no existe todavía")
    return client


def _client_with_existing_table(expiration_ms: int | None):
    """Simula una tabla que ya existe, con una expiración de partición
    puntual (o None si nunca se configuró)."""
    client = MagicMock(spec=bigquery.Client)
    client.project = "proj"
    existing = bigquery.Table(
        bigquery.DatasetReference("proj", "sepa").table("productos")
    )
    existing.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY, field="dt", expiration_ms=expiration_ms
    )
    client.get_table.return_value = existing
    return client


def test_ensure_table_sets_partitioning_clustering_and_expiration_on_create():
    client = _client_without_table()

    ensure_table(client, "sepa", "productos")

    client.create_table.assert_called_once()
    created_table = client.create_table.call_args.args[0]
    assert created_table.time_partitioning.field == "dt"
    assert created_table.time_partitioning.type_ == bigquery.TimePartitioningType.DAY
    assert created_table.time_partitioning.expiration_ms == THREE_DAYS_MS
    assert created_table.clustering_fields == ["id_comercio"]
    client.update_table.assert_not_called()


def test_ensure_table_respects_custom_expiration_days_on_create():
    client = _client_without_table()

    ensure_table(client, "sepa", "productos", partition_expiration_days=7)

    created_table = client.create_table.call_args.args[0]
    assert created_table.time_partitioning.expiration_ms == 7 * 24 * 60 * 60 * 1000


def test_ensure_table_none_means_no_expiration_on_create():
    client = _client_without_table()

    ensure_table(client, "sepa", "productos", partition_expiration_days=None)

    created_table = client.create_table.call_args.args[0]
    assert created_table.time_partitioning.expiration_ms is None


def test_ensure_table_updates_expiration_on_existing_table_without_recreating():
    # la tabla ya existe sin expiración (None) - al pedir 3 días, se tiene
    # que actualizar, NUNCA recrear (perdería las particiones ya cargadas).
    client = _client_with_existing_table(expiration_ms=None)

    ensure_table(client, "sepa", "productos")

    client.create_table.assert_not_called()
    client.update_table.assert_called_once()
    updated_table, fields = client.update_table.call_args.args
    assert updated_table.time_partitioning.expiration_ms == THREE_DAYS_MS
    assert fields == ["time_partitioning"]


def test_ensure_table_is_noop_if_expiration_already_matches():
    # ya tiene puestos los 3 días - no hay nada para actualizar.
    client = _client_with_existing_table(expiration_ms=THREE_DAYS_MS)

    ensure_table(client, "sepa", "productos")

    client.create_table.assert_not_called()
    client.update_table.assert_not_called()


def test_default_partition_expiration_days_is_3():
    assert DEFAULT_PARTITION_EXPIRATION_DAYS == 3


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
    client = _client_without_table()

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
