#!/usr/bin/env python3
"""CLI para correr el staging de un día puntual.

Uso: python scripts/stage_sepa.py --date 2026-11-19
"""
from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from pipeline.stage import (  # noqa: E402
    DEFAULT_MAX_BAD_ROW_RATIO,
    DEFAULT_MAX_LAG_DAYS,
    find_daily_zip,
    run_staging,
)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--raw-dir", default=str(REPO_ROOT / "data" / "raw"))
    parser.add_argument("--staging-dir", default=str(REPO_ROOT / "data" / "staging"))
    parser.add_argument(
        "--max-bad-row-ratio",
        type=float,
        default=DEFAULT_MAX_BAD_ROW_RATIO,
        help=(
            "Proporción máxima de filas descartadas+corregidas antes de "
            "cuarentenar el comercio (default 1%%)"
        ),
    )
    parser.add_argument(
        "--max-lag-days",
        type=int,
        default=DEFAULT_MAX_LAG_DAYS,
        help="Días de atraso tolerados en fecha_archivo antes de cuarentenar el comercio (default 1)",
    )
    args = parser.parse_args()

    partition_date = date.fromisoformat(args.date)
    daily_zip_path = find_daily_zip(Path(args.raw_dir), partition_date)

    result = run_staging(
        daily_zip_path,
        partition_date,
        Path(args.staging_dir),
        max_bad_row_ratio=args.max_bad_row_ratio,
        max_lag_days=args.max_lag_days,
    )

    print(f"partición: {result.partition_date}")
    print(f"comercios ok: {len(result.ok_comercio_ids)} -> {result.ok_comercio_ids}")
    print(f"comercios en cuarentena: {len(result.quarantined)}")
    for q in result.quarantined:
        print(f"  - comercio {q.comercio_id}: {q.reason}")
    print(f"comercios inactivos (no se repiten en cuarentena diaria): {len(result.inactive_comercios)}")
    for c in result.inactive_comercios:
        print(f"  - comercio {c.comercio_id}: sin actualizar desde {c.fecha_archivo} ({c.lag_days} días)")
    print(f"filas escritas: {result.rows_written}")
    total_discarded = sum(r["rows_discarded"] for r in result.quality_report)
    total_corrected = sum(r["rows_corrected"] for r in result.quality_report)
    print(f"filas descartadas (precio_lista nulo/negativo): {total_discarded}")
    print(f"filas corregidas (precio secundario negativo anulado): {total_corrected}")


if __name__ == "__main__":
    main()
