import io
import json
import zipfile
from datetime import date

import pandas as pd
import pytest

from pipeline.stage import find_daily_zip, run_staging

PARTITION_DATE = date(2026, 11, 19)


def _comercio_subzip(
    comercio_id: int,
    *,
    fecha_archivo: str = "2026-11-19T09:00:00-03:00",
    productos_rows: list[tuple[str, str]] | None = None,
) -> bytes:
    """productos_rows: lista de (precio_lista, precio_referencia) como string,
    una fila de productos.csv por tupla."""
    if productos_rows is None:
        productos_rows = [("100.0", "100.0")]

    comercio_csv = (
        f"id_comercio|comercio_razon_social\r\n"
        f"{comercio_id}|Comercio {comercio_id} S.A.\r\n"
        f"\r\n"
        f"Última actualización: {fecha_archivo}\r\n"
    ).encode()
    sucursales_csv = (
        f"id_comercio|id_sucursal\r\n{comercio_id}|100\r\n\r\n"
        f"Última actualización: {fecha_archivo}\r\n"
    ).encode()

    header = (
        "id_comercio|id_producto|productos_precio_lista|productos_precio_referencia|"
        "productos_precio_unitario_promo1|productos_precio_unitario_promo2\r\n"
    )
    rows = "".join(
        f"{comercio_id}|{i}|{precio_lista}|{precio_referencia}||\r\n"
        for i, (precio_lista, precio_referencia) in enumerate(productos_rows, start=1)
    )
    productos_csv = (header + rows + f"\r\nÚltima actualización: {fecha_archivo}\r\n").encode("utf-8")

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("comercio.csv", comercio_csv)
        z.writestr("sucursales.csv", sucursales_csv)
        z.writestr("productos.csv", productos_csv)
    return buf.getvalue()


def _write_daily_zip(raw_dir, entries: dict[str, bytes]) -> None:
    partition_dir = raw_dir / "sepa" / f"dt={PARTITION_DATE.isoformat()}"
    partition_dir.mkdir(parents=True, exist_ok=True)
    daily_zip_path = partition_dir / "sepa_jueves.zip"
    with zipfile.ZipFile(daily_zip_path, "w") as daily_zip:
        for name, content in entries.items():
            daily_zip.writestr(name, content)


@pytest.fixture
def raw_dir(tmp_path):
    raw_dir = tmp_path / "raw"
    _write_daily_zip(
        raw_dir,
        {
            # comercio 1: sano.
            "sepa_1_comercio-sepa-1_2026-11-19_09-05-10.zip": _comercio_subzip(1),
            # comercio 2: ZIP roto (problema estructural, no de filas).
            "sepa_1_comercio-sepa-2_2026-11-19_09-05-10.zip": b"",
            # comercio 3: 1 de 1 filas con precio_lista negativo -> 100% malo,
            # supera cualquier umbral razonable -> cuarentena por umbral.
            "sepa_1_comercio-sepa-3_2026-11-19_09-05-10.zip": _comercio_subzip(
                3, productos_rows=[("-5.0", "")]
            ),
        },
    )
    return raw_dir


def test_find_daily_zip_locates_the_single_zip(raw_dir):
    found = find_daily_zip(raw_dir, PARTITION_DATE)
    assert found.name == "sepa_jueves.zip"


def test_run_staging_isolates_broken_zip_and_comercio_over_threshold(tmp_path, raw_dir):
    staging_dir = tmp_path / "staging"
    daily_zip_path = find_daily_zip(raw_dir, PARTITION_DATE)

    result = run_staging(daily_zip_path, PARTITION_DATE, staging_dir)

    assert result.ok_comercio_ids == [1]
    quarantined_ids = {q.comercio_id for q in result.quarantined}
    assert quarantined_ids == {2, 3}

    productos = pd.read_parquet(staging_dir / "productos" / f"dt={PARTITION_DATE.isoformat()}")
    assert len(productos) == 1
    assert productos.loc[0, "productos_precio_lista"] == 100.0


def test_run_staging_rescues_comercio_under_threshold_discarding_only_bad_rows(tmp_path):
    """Caso real que motivó este cambio: un comercio grande con una fracción
    chica de filas rotas (precio_lista nulo) no debería perder TODAS sus
    filas buenas."""
    raw_dir = tmp_path / "raw"
    # 99 filas sanas + 1 con precio_lista nulo = 1% de filas malas exactas.
    rows = [("100.0", "100.0")] * 99 + [("", "100.0")]
    _write_daily_zip(
        raw_dir,
        {"sepa_1_comercio-sepa-12_2026-11-19_09-05-10.zip": _comercio_subzip(12, productos_rows=rows)},
    )
    staging_dir = tmp_path / "staging"
    daily_zip_path = find_daily_zip(raw_dir, PARTITION_DATE)

    result = run_staging(daily_zip_path, PARTITION_DATE, staging_dir, max_bad_row_ratio=0.01)

    assert result.ok_comercio_ids == [12]  # NO va a cuarentena completa
    assert result.quarantined == []

    productos = pd.read_parquet(staging_dir / "productos" / f"dt={PARTITION_DATE.isoformat()}")
    assert len(productos) == 99  # se descartó solo la fila mala

    report = {r["comercio_id"]: r for r in result.quality_report}[12]
    assert report["rows_read"] == 100
    assert report["rows_accepted"] == 99
    assert report["rows_discarded"] == 1
    assert report["rows_corrected"] == 0
    assert report["quarantined"] is False


def test_run_staging_keeps_row_and_nulls_negative_referencia(tmp_path):
    raw_dir = tmp_path / "raw"
    # 99 filas sanas + 1 con precio_referencia negativo = 1% corregido,
    # igual que el caso real (comercio 15 / DIA: 969 de 4.334.549 ~ 0.02%).
    rows = [("100.0", "100.0")] * 99 + [("50.0", "-28447.37")]
    _write_daily_zip(
        raw_dir,
        {"sepa_1_comercio-sepa-15_2026-11-19_09-05-10.zip": _comercio_subzip(15, productos_rows=rows)},
    )
    staging_dir = tmp_path / "staging"
    daily_zip_path = find_daily_zip(raw_dir, PARTITION_DATE)

    result = run_staging(daily_zip_path, PARTITION_DATE, staging_dir)

    assert result.ok_comercio_ids == [15]
    productos = pd.read_parquet(staging_dir / "productos" / f"dt={PARTITION_DATE.isoformat()}")
    assert len(productos) == 100  # ninguna fila se descartó

    fixed_row = productos[productos["id_producto"] == "100"].iloc[0]
    assert pd.isna(fixed_row["productos_precio_referencia"])
    assert fixed_row["productos_precio_lista"] == 50.0  # intacto

    report = {r["comercio_id"]: r for r in result.quality_report}[15]
    assert report["rows_corrected"] == 1
    assert report["rows_discarded"] == 0


def test_run_staging_writes_quarantine_log(tmp_path, raw_dir):
    staging_dir = tmp_path / "staging"
    daily_zip_path = find_daily_zip(raw_dir, PARTITION_DATE)

    run_staging(daily_zip_path, PARTITION_DATE, staging_dir)

    log_path = staging_dir / "_quarantine" / f"dt={PARTITION_DATE.isoformat()}.jsonl"
    assert log_path.exists()
    lines = log_path.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 2


def test_run_staging_writes_row_level_quarantine_log_with_raw_row(tmp_path):
    raw_dir = tmp_path / "raw"
    rows = [("100.0", "100.0")] * 99 + [("", "100.0")]
    _write_daily_zip(
        raw_dir,
        {"sepa_1_comercio-sepa-12_2026-11-19_09-05-10.zip": _comercio_subzip(12, productos_rows=rows)},
    )
    staging_dir = tmp_path / "staging"
    daily_zip_path = find_daily_zip(raw_dir, PARTITION_DATE)

    run_staging(daily_zip_path, PARTITION_DATE, staging_dir, max_bad_row_ratio=0.01)

    log_path = staging_dir / "_quarantine_rows" / f"dt={PARTITION_DATE.isoformat()}.jsonl"
    lines = [json.loads(line) for line in log_path.read_text(encoding="utf-8").strip().splitlines()]
    assert len(lines) == 1
    assert lines[0]["comercio_id"] == 12
    assert lines[0]["reason"] == "productos_precio_lista nulo"
    assert lines[0]["row"]["productos_precio_lista"] is None


def test_run_staging_writes_quality_report_csv(tmp_path, raw_dir):
    staging_dir = tmp_path / "staging"
    daily_zip_path = find_daily_zip(raw_dir, PARTITION_DATE)

    run_staging(daily_zip_path, PARTITION_DATE, staging_dir)

    report_path = staging_dir / "_quality_report" / f"dt={PARTITION_DATE.isoformat()}.csv"
    df = pd.read_csv(report_path)
    assert set(df["comercio_id"]) == {1, 2, 3}
    assert set(df.columns) == {
        "comercio_id",
        "entry_name",
        "rows_read",
        "rows_accepted",
        "rows_discarded",
        "rows_corrected",
        "bad_ratio",
        "quarantined",
        "inactive",
        "quarantine_reason",
    }


# --- fecha_archivo: columna + tolerancia de atraso + inactivos -----------


def test_productos_parquet_has_fecha_archivo_column_not_partition_date(tmp_path):
    """fecha_archivo tiene que ser la fecha real del comercio (footer), no
    la fecha de la partición - son distintas cuando el comercio está
    atrasado dentro de la tolerancia."""
    raw_dir = tmp_path / "raw"
    _write_daily_zip(
        raw_dir,
        {
            "sepa_1_comercio-sepa-1_2026-11-19_09-05-10.zip": _comercio_subzip(
                1, fecha_archivo="2026-11-18T09:00:00-03:00"
            )
        },
    )
    staging_dir = tmp_path / "staging"
    daily_zip_path = find_daily_zip(raw_dir, PARTITION_DATE)

    result = run_staging(daily_zip_path, PARTITION_DATE, staging_dir, max_lag_days=1)

    assert result.ok_comercio_ids == [1]  # 1 día de atraso, dentro de la tolerancia default
    productos = pd.read_parquet(staging_dir / "productos" / f"dt={PARTITION_DATE.isoformat()}")
    assert productos.loc[0, "fecha_archivo"] == "2026-11-18"  # no "2026-11-19" (la partición)


def test_comercio_beyond_max_lag_days_is_quarantined(tmp_path):
    raw_dir = tmp_path / "raw"
    _write_daily_zip(
        raw_dir,
        {
            "sepa_1_comercio-sepa-1_2026-11-19_09-05-10.zip": _comercio_subzip(
                1, fecha_archivo="2026-11-15T09:00:00-03:00"  # 4 días de atraso
            )
        },
    )
    staging_dir = tmp_path / "staging"
    daily_zip_path = find_daily_zip(raw_dir, PARTITION_DATE)

    result = run_staging(daily_zip_path, PARTITION_DATE, staging_dir, max_lag_days=1)

    assert result.ok_comercio_ids == []
    assert len(result.quarantined) == 1
    assert "días de atraso" in result.quarantined[0].reason


def test_max_lag_days_is_configurable(tmp_path):
    raw_dir = tmp_path / "raw"
    _write_daily_zip(
        raw_dir,
        {
            "sepa_1_comercio-sepa-1_2026-11-19_09-05-10.zip": _comercio_subzip(
                1, fecha_archivo="2026-11-15T09:00:00-03:00"  # 4 días de atraso
            )
        },
    )
    staging_dir = tmp_path / "staging"
    daily_zip_path = find_daily_zip(raw_dir, PARTITION_DATE)

    result = run_staging(daily_zip_path, PARTITION_DATE, staging_dir, max_lag_days=7)

    assert result.ok_comercio_ids == [1]
    assert result.quarantined == []


def test_inactive_comercio_is_not_written_to_daily_quarantine_log(tmp_path):
    """Caso real: comercio 47, sin actualizar desde 2025-06-11. No debe
    aparecer en _quarantine/ (ahí se repetiría todos los días) sino en
    _inactive_comercios/."""
    raw_dir = tmp_path / "raw"
    _write_daily_zip(
        raw_dir,
        {
            "sepa_1_comercio-sepa-47_2026-11-19_09-05-10.zip": _comercio_subzip(
                47, fecha_archivo="2025-06-11T21:00:01-03:00"
            )
        },
    )
    staging_dir = tmp_path / "staging"
    daily_zip_path = find_daily_zip(raw_dir, PARTITION_DATE)

    result = run_staging(daily_zip_path, PARTITION_DATE, staging_dir)

    assert result.ok_comercio_ids == []
    assert result.quarantined == []  # no está en la cuarentena diaria
    assert len(result.inactive_comercios) == 1
    assert result.inactive_comercios[0].comercio_id == 47
    assert result.inactive_comercios[0].fecha_archivo == date(2025, 6, 11)

    quarantine_log = staging_dir / "_quarantine" / f"dt={PARTITION_DATE.isoformat()}.jsonl"
    assert quarantine_log.read_text(encoding="utf-8").strip() == ""

    inactive_log = staging_dir / "_inactive_comercios" / f"dt={PARTITION_DATE.isoformat()}.jsonl"
    entry = json.loads(inactive_log.read_text(encoding="utf-8").strip())
    assert entry == {
        "comercio_id": 47,
        "entry_name": "sepa_1_comercio-sepa-47_2026-11-19_09-05-10.zip",
        "fecha_archivo": "2025-06-11",
        "lag_days": (PARTITION_DATE - date(2025, 6, 11)).days,
    }

    report = {r["comercio_id"]: r for r in result.quality_report}[47]
    assert report["inactive"] is True
    assert report["quarantined"] is False
