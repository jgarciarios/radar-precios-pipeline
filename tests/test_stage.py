import io
import zipfile
from datetime import date

import pandas as pd
import pytest

from pipeline.stage import find_daily_zip, run_staging

PARTITION_DATE = date(2026, 11, 19)


def _comercio_subzip(comercio_id: int, *, fecha_archivo: str, precio_lista: str) -> bytes:
    comercio_csv = (
        f"id_comercio|comercio_razon_social\r\n"
        f"{comercio_id}|Comercio {comercio_id} S.A.\r\n"
        f"\r\n"
        f"Última actualización: {fecha_archivo}\r\n"
    ).encode("utf-8")
    sucursales_csv = (
        f"id_comercio|id_sucursal\r\n{comercio_id}|100\r\n\r\n"
        f"Última actualización: {fecha_archivo}\r\n"
    ).encode("utf-8")
    productos_csv = (
        "id_comercio|id_producto|productos_precio_lista|productos_precio_referencia|"
        "productos_precio_unitario_promo1|productos_precio_unitario_promo2\r\n"
        f"{comercio_id}|1|{precio_lista}|{precio_lista}||\r\n\r\n"
        f"Última actualización: {fecha_archivo}\r\n"
    ).encode("utf-8")

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("comercio.csv", comercio_csv)
        z.writestr("sucursales.csv", sucursales_csv)
        z.writestr("productos.csv", productos_csv)
    return buf.getvalue()


@pytest.fixture
def raw_dir(tmp_path):
    raw_dir = tmp_path / "raw"
    partition_dir = raw_dir / "sepa" / f"dt={PARTITION_DATE.isoformat()}"
    partition_dir.mkdir(parents=True)

    daily_zip_path = partition_dir / "sepa_jueves.zip"
    with zipfile.ZipFile(daily_zip_path, "w") as daily_zip:
        # comercio 1: sano.
        daily_zip.writestr(
            "sepa_1_comercio-sepa-1_2026-11-19_09-05-10.zip",
            _comercio_subzip(1, fecha_archivo="2026-11-19T09:00:00-03:00", precio_lista="100.0"),
        )
        # comercio 2: ZIP roto.
        daily_zip.writestr("sepa_1_comercio-sepa-2_2026-11-19_09-05-10.zip", b"")
        # comercio 3: precio negativo -> falla validación.
        daily_zip.writestr(
            "sepa_1_comercio-sepa-3_2026-11-19_09-05-10.zip",
            _comercio_subzip(3, fecha_archivo="2026-11-19T09:00:00-03:00", precio_lista="-5.0"),
        )

    return raw_dir


def test_find_daily_zip_locates_the_single_zip(raw_dir):
    found = find_daily_zip(raw_dir, PARTITION_DATE)
    assert found.name == "sepa_jueves.zip"


def test_run_staging_isolates_broken_and_invalid_comercios(tmp_path, raw_dir):
    staging_dir = tmp_path / "staging"
    daily_zip_path = find_daily_zip(raw_dir, PARTITION_DATE)

    result = run_staging(daily_zip_path, PARTITION_DATE, staging_dir)

    assert result.ok_comercio_ids == [1]
    quarantined_ids = {q.comercio_id for q in result.quarantined}
    assert quarantined_ids == {2, 3}

    productos = pd.read_parquet(staging_dir / "productos" / f"dt={PARTITION_DATE.isoformat()}")
    assert len(productos) == 1
    assert productos.loc[0, "productos_precio_lista"] == 100.0


def test_run_staging_writes_quarantine_log(tmp_path, raw_dir):
    staging_dir = tmp_path / "staging"
    daily_zip_path = find_daily_zip(raw_dir, PARTITION_DATE)

    run_staging(daily_zip_path, PARTITION_DATE, staging_dir)

    log_path = staging_dir / "_quarantine" / f"dt={PARTITION_DATE.isoformat()}.jsonl"
    assert log_path.exists()
    lines = log_path.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 2
