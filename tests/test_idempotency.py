"""Correr el staging dos veces para el mismo partition_date tiene que dar
exactamente el mismo resultado: cada corrida recalcula todo desde cero y
pisa la partición completa, no hay append. Reutiliza el fixture `raw_dir`
de test_stage.py (misma carpeta raw simulada: comercio sano + roto + con
precio negativo)."""
import pandas as pd

from pipeline.stage import find_daily_zip, run_staging
from tests.test_stage import PARTITION_DATE, raw_dir  # noqa: F401 (fixture)


def test_running_staging_twice_yields_identical_output(tmp_path, raw_dir):  # noqa: F811
    staging_dir = tmp_path / "staging"
    daily_zip_path = find_daily_zip(raw_dir, PARTITION_DATE)

    first = run_staging(daily_zip_path, PARTITION_DATE, staging_dir)
    productos_after_first = pd.read_parquet(
        staging_dir / "productos" / f"dt={PARTITION_DATE.isoformat()}"
    )

    second = run_staging(daily_zip_path, PARTITION_DATE, staging_dir)
    productos_after_second = pd.read_parquet(
        staging_dir / "productos" / f"dt={PARTITION_DATE.isoformat()}"
    )

    assert first.ok_comercio_ids == second.ok_comercio_ids
    assert first.rows_written == second.rows_written
    assert len(first.quarantined) == len(second.quarantined)

    pd.testing.assert_frame_equal(
        productos_after_first.reset_index(drop=True),
        productos_after_second.reset_index(drop=True),
    )

    log_path = staging_dir / "_quarantine" / f"dt={PARTITION_DATE.isoformat()}.jsonl"
    lines = log_path.read_text(encoding="utf-8").strip().splitlines()
    # sin duplicar: sigue habiendo una línea por comercio en cuarentena, no el doble.
    assert len(lines) == len(second.quarantined)
