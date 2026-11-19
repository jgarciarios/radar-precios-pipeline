"""Orquesta el pipeline raw -> staging para un día: desanida, parsea, valida
y escribe Parquet particionado por fecha.

Cuarentena en tres niveles (ver pipeline.validate para el porqué):
  - Por FILA: una fila de productos.csv sin precio principal usable se
    descarta y se registra en _quarantine_rows/. Una fila con un precio
    secundario negativo se conserva con ese campo anulado.
  - Por COMERCIO (diaria): un comercio va entero a _quarantine/ solo si
    falla una regla estructural (tabla vacía, atraso de fecha mayor a
    max_lag_days) o si la proporción de filas descartadas+corregidas
    supera max_bad_row_ratio.
  - Por COMERCIO (inactivo): un comercio sin actualizar hace más de un año
    no es "cuarentena de hoy" - es una condición estable. Va a
    _inactive_comercios/ en vez de _quarantine/, para no repetir la misma
    alerta todos los días en el log diario.

La fecha real de cada comercio (el footer de comercio.csv) queda como
columna `fecha_archivo` en productos - `dt=` sigue siendo la fecha en que
se *descargó* el ZIP (fase 1), que puede ser distinta de la fecha real de
los datos de un comercio puntual.

El resultado final (staging/<tabla>/dt=.../) se escribe de forma atómica:
se arma en un directorio temporal y recién al final se reemplaza el
directorio definitivo con os.rename (atómico en el mismo filesystem, mismo
patrón que la escritura del ZIP en la fase 1).

Correr esto dos veces para el mismo partition_date da exactamente el mismo
resultado: no hay acumulación/append, cada corrida recalcula todo desde el
ZIP raw y pisa por completo la partición de ese día.
"""
from __future__ import annotations

import json
import os
import shutil
import tempfile
import zipfile
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

import pandas as pd

from pipeline.parse import parse_comercio_csv, parse_productos_csv, parse_sucursales_csv
from pipeline.unzip import ComercioEntry, list_comercio_entries, read_comercio_csvs
from pipeline.validate import (
    DEFAULT_MAX_LAG_DAYS,
    apply_row_level_price_rules,
    is_inactive,
    validate_structural,
)

TABLES = ("comercio", "sucursales", "productos")
DEFAULT_MAX_BAD_ROW_RATIO = 0.01


@dataclass
class QuarantinedComercio:
    comercio_id: int
    entry_name: str
    reason: str


@dataclass
class InactiveComercio:
    comercio_id: int
    entry_name: str
    fecha_archivo: date
    lag_days: int


@dataclass
class ComercioOutcome:
    comercio_id: int
    entry_name: str
    quarantined: bool
    quarantine_reason: str | None
    rows_read: int
    rows_accepted: int
    rows_discarded: int
    rows_corrected: int
    bad_ratio: float | None  # None si se cuarentenó antes de poder calcularlo
    inactive: bool = False
    inactive_info: InactiveComercio | None = None
    comercio_df: pd.DataFrame | None = None
    sucursales_df: pd.DataFrame | None = None
    productos_df: pd.DataFrame | None = None  # ya limpio, solo si no quarantined ni inactive
    discarded_rows_log: list[dict] = field(default_factory=list)


@dataclass
class StageResult:
    partition_date: date
    rows_written: dict[str, int] = field(default_factory=dict)
    ok_comercio_ids: list[int] = field(default_factory=list)
    quarantined: list[QuarantinedComercio] = field(default_factory=list)
    inactive_comercios: list[InactiveComercio] = field(default_factory=list)
    quality_report: list[dict] = field(default_factory=list)


def find_daily_zip(raw_dir: Path, partition_date: date) -> Path:
    partition_dir = raw_dir / "sepa" / f"dt={partition_date.isoformat()}"
    matches = sorted(partition_dir.glob("*.zip"))
    if len(matches) != 1:
        raise FileNotFoundError(
            f"Esperaba exactamente 1 ZIP en {partition_dir}, encontré {len(matches)}: {matches}"
        )
    return matches[0]


def _process_comercio(
    daily_zip_path: Path,
    entry: ComercioEntry,
    partition_date: date,
    max_bad_row_ratio: float,
    max_lag_days: int = DEFAULT_MAX_LAG_DAYS,
) -> ComercioOutcome:
    try:
        csvs = read_comercio_csvs(daily_zip_path, entry)
    except (zipfile.BadZipFile, KeyError) as exc:
        return ComercioOutcome(
            comercio_id=entry.comercio_id,
            entry_name=entry.entry_name,
            quarantined=True,
            quarantine_reason=f"{type(exc).__name__}: {exc}",
            rows_read=0,
            rows_accepted=0,
            rows_discarded=0,
            rows_corrected=0,
            bad_ratio=None,
        )

    comercio_df, fecha_archivo = parse_comercio_csv(csvs["comercio.csv"])
    sucursales_df = parse_sucursales_csv(csvs["sucursales.csv"])
    productos_df = parse_productos_csv(csvs["productos.csv"])
    # Fecha real del comercio, no la de la partición (que es la fecha de
    # descarga del ZIP, fase 1) - se conserva en el dato en vez de asumir
    # que todos los comercios están "al día" con la partición.
    productos_df = productos_df.assign(
        fecha_archivo=fecha_archivo.isoformat() if fecha_archivo else None
    )
    rows_read = len(productos_df)

    if is_inactive(fecha_archivo, partition_date):
        lag_days = (partition_date - fecha_archivo).days
        return ComercioOutcome(
            comercio_id=entry.comercio_id,
            entry_name=entry.entry_name,
            quarantined=False,
            quarantine_reason=None,
            rows_read=rows_read,
            rows_accepted=0,
            rows_discarded=0,
            rows_corrected=0,
            bad_ratio=None,
            inactive=True,
            inactive_info=InactiveComercio(
                comercio_id=entry.comercio_id,
                entry_name=entry.entry_name,
                fecha_archivo=fecha_archivo,
                lag_days=lag_days,
            ),
        )

    structural_errors = validate_structural(
        comercio_df=comercio_df,
        sucursales_df=sucursales_df,
        productos_df=productos_df,
        fecha_archivo=fecha_archivo,
        partition_date=partition_date,
        max_lag_days=max_lag_days,
    )
    if structural_errors:
        return ComercioOutcome(
            comercio_id=entry.comercio_id,
            entry_name=entry.entry_name,
            quarantined=True,
            quarantine_reason="; ".join(structural_errors),
            rows_read=rows_read,
            rows_accepted=0,
            rows_discarded=0,
            rows_corrected=0,
            bad_ratio=None,
        )

    row_outcome = apply_row_level_price_rules(productos_df)
    rows_discarded = len(row_outcome.discarded_rows)
    rows_corrected = row_outcome.rows_corrected
    bad_ratio = (rows_discarded + rows_corrected) / rows_read if rows_read else 0.0

    if bad_ratio > max_bad_row_ratio:
        return ComercioOutcome(
            comercio_id=entry.comercio_id,
            entry_name=entry.entry_name,
            quarantined=True,
            quarantine_reason=(
                f"{bad_ratio:.2%} de filas con problemas de precio supera "
                f"el umbral configurado ({max_bad_row_ratio:.2%})"
            ),
            rows_read=rows_read,
            rows_accepted=0,
            rows_discarded=rows_discarded,
            rows_corrected=rows_corrected,
            bad_ratio=bad_ratio,
        )

    return ComercioOutcome(
        comercio_id=entry.comercio_id,
        entry_name=entry.entry_name,
        quarantined=False,
        quarantine_reason=None,
        rows_read=rows_read,
        rows_accepted=len(row_outcome.cleaned_df),
        rows_discarded=rows_discarded,
        rows_corrected=rows_corrected,
        bad_ratio=bad_ratio,
        comercio_df=comercio_df,
        sucursales_df=sucursales_df,
        productos_df=row_outcome.cleaned_df,
        discarded_rows_log=row_outcome.discarded_rows,
    )


def _atomic_write_partition(df: pd.DataFrame, table_dir: Path, partition_date: date) -> None:
    final_dir = table_dir / f"dt={partition_date.isoformat()}"
    table_dir.mkdir(parents=True, exist_ok=True)

    tmp_dir = Path(tempfile.mkdtemp(prefix=".tmp-", dir=table_dir))
    df.to_parquet(tmp_dir / "part-000.parquet", index=False)

    if final_dir.exists():
        shutil.rmtree(final_dir)
    os.rename(tmp_dir, final_dir)  # atómico: mismo filesystem, misma operación que en la fase 1


def _atomic_write_jsonl(path: Path, lines: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_path_str = tempfile.mkstemp(prefix=".tmp-", dir=path.parent, suffix=".jsonl")
    tmp_path = Path(tmp_path_str)
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        for line in lines:
            f.write(json.dumps(line, ensure_ascii=False) + "\n")
    os.replace(tmp_path, path)


def _write_quarantine_log(
    staging_dir: Path, partition_date: date, quarantined: list[QuarantinedComercio]
) -> None:
    path = staging_dir / "_quarantine" / f"dt={partition_date.isoformat()}.jsonl"
    lines = [
        {"comercio_id": q.comercio_id, "entry_name": q.entry_name, "reason": q.reason}
        for q in quarantined
    ]
    _atomic_write_jsonl(path, lines)


def _write_row_quarantine_log(
    staging_dir: Path, partition_date: date, row_entries: list[dict]
) -> None:
    path = staging_dir / "_quarantine_rows" / f"dt={partition_date.isoformat()}.jsonl"
    _atomic_write_jsonl(path, row_entries)


def _write_inactive_log(
    staging_dir: Path, partition_date: date, inactive_comercios: list[InactiveComercio]
) -> None:
    path = staging_dir / "_inactive_comercios" / f"dt={partition_date.isoformat()}.jsonl"
    lines = [
        {
            "comercio_id": c.comercio_id,
            "entry_name": c.entry_name,
            "fecha_archivo": c.fecha_archivo.isoformat(),
            "lag_days": c.lag_days,
        }
        for c in inactive_comercios
    ]
    _atomic_write_jsonl(path, lines)


def _write_quality_report(staging_dir: Path, partition_date: date, quality_rows: list[dict]) -> None:
    report_dir = staging_dir / "_quality_report"
    report_dir.mkdir(parents=True, exist_ok=True)
    final_path = report_dir / f"dt={partition_date.isoformat()}.csv"

    fd, tmp_path_str = tempfile.mkstemp(prefix=".tmp-", dir=report_dir, suffix=".csv")
    os.close(fd)
    tmp_path = Path(tmp_path_str)
    pd.DataFrame(quality_rows).to_csv(tmp_path, index=False)
    os.replace(tmp_path, final_path)


def run_staging(
    daily_zip_path: Path,
    partition_date: date,
    staging_dir: Path,
    max_bad_row_ratio: float = DEFAULT_MAX_BAD_ROW_RATIO,
    max_lag_days: int = DEFAULT_MAX_LAG_DAYS,
) -> StageResult:
    result = StageResult(partition_date=partition_date)
    entries = list_comercio_entries(daily_zip_path)

    comercio_frames, sucursales_frames, productos_frames = [], [], []
    row_quarantine_entries: list[dict] = []

    for entry in entries:
        outcome = _process_comercio(
            daily_zip_path, entry, partition_date, max_bad_row_ratio, max_lag_days
        )

        result.quality_report.append(
            {
                "comercio_id": outcome.comercio_id,
                "entry_name": outcome.entry_name,
                "rows_read": outcome.rows_read,
                "rows_accepted": outcome.rows_accepted,
                "rows_discarded": outcome.rows_discarded,
                "rows_corrected": outcome.rows_corrected,
                "bad_ratio": outcome.bad_ratio,
                "quarantined": outcome.quarantined,
                "inactive": outcome.inactive,
                "quarantine_reason": outcome.quarantine_reason,
            }
        )

        if outcome.inactive:
            result.inactive_comercios.append(outcome.inactive_info)
            continue

        if outcome.quarantined:
            result.quarantined.append(
                QuarantinedComercio(outcome.comercio_id, outcome.entry_name, outcome.quarantine_reason)
            )
            continue

        comercio_frames.append(outcome.comercio_df)
        sucursales_frames.append(outcome.sucursales_df)
        productos_frames.append(outcome.productos_df)
        result.ok_comercio_ids.append(outcome.comercio_id)

        for discard in outcome.discarded_rows_log:
            row_quarantine_entries.append({"comercio_id": outcome.comercio_id, **discard})

    tables = {
        "comercio": pd.concat(comercio_frames, ignore_index=True) if comercio_frames else pd.DataFrame(),
        "sucursales": pd.concat(sucursales_frames, ignore_index=True) if sucursales_frames else pd.DataFrame(),
        "productos": pd.concat(productos_frames, ignore_index=True) if productos_frames else pd.DataFrame(),
    }

    for table_name, df in tables.items():
        _atomic_write_partition(df, staging_dir / table_name, partition_date)
        result.rows_written[table_name] = len(df)

    _write_quarantine_log(staging_dir, partition_date, result.quarantined)
    _write_row_quarantine_log(staging_dir, partition_date, row_quarantine_entries)
    _write_inactive_log(staging_dir, partition_date, result.inactive_comercios)
    _write_quality_report(staging_dir, partition_date, result.quality_report)

    return result
