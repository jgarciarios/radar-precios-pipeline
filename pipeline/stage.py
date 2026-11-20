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

productos.csv del comercio más grande se procesa por chunks (ver
pipeline.parse.parse_productos_csv_chunks) en vez de materializarlo
entero: cada chunk se valida y se escribe a un archivo Parquet propio
apenas se procesa, nunca se tiene más de un chunk en memoria a la vez (ver
README, fase 4b, para la medición de memoria antes/después de este
cambio). La decisión de cuarentenar el comercio completo (por bad_ratio)
puede tomarse recién después de ver todos sus chunks, así que esos
archivos se escriben primero a un directorio de scratch por comercio y
recién se "confirman" (se mueven al directorio final) si el comercio pasa
- si no pasa, el scratch se borra entero y no deja rastro.

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

from pipeline.parse import (
    DEFAULT_PRODUCTOS_CHUNK_SIZE,
    parse_comercio_csv,
    parse_productos_csv_chunks,
    parse_sucursales_csv,
)
from pipeline.unzip import ComercioEntry, list_comercio_entries, read_comercio_csvs
from pipeline.validate import (
    DEFAULT_MAX_LAG_DAYS,
    apply_row_level_price_rules,
    is_inactive,
    validate_comercio_tables,
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
    # Parquet ya escritos (uno por chunk) en un directorio de scratch,
    # solo si no quarantined ni inactive - run_staging los mueve al
    # directorio final y borra el scratch (que queda vacío). Nunca hay un
    # productos_df completo en memoria acá.
    productos_chunk_paths: list[Path] = field(default_factory=list)
    productos_scratch_dir: Path | None = None
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


def _count_productos_rows(raw: bytes, chunk_size_rows: int) -> int:
    """Cuenta filas sin mantener más de un chunk en memoria a la vez -
    usado en los caminos de salida temprana (inactivo, error estructural)
    donde igual se necesita el conteo real para el reporte de calidad."""
    return sum(len(chunk) for chunk in parse_productos_csv_chunks(raw, chunk_size_rows=chunk_size_rows))


def _empty_outcome(entry: ComercioEntry, reason: str, rows_read: int = 0) -> ComercioOutcome:
    return ComercioOutcome(
        comercio_id=entry.comercio_id,
        entry_name=entry.entry_name,
        quarantined=True,
        quarantine_reason=reason,
        rows_read=rows_read,
        rows_accepted=0,
        rows_discarded=0,
        rows_corrected=0,
        bad_ratio=None,
    )


def _process_comercio(
    daily_zip_path: Path,
    entry: ComercioEntry,
    partition_date: date,
    max_bad_row_ratio: float,
    max_lag_days: int = DEFAULT_MAX_LAG_DAYS,
    chunk_size_rows: int = DEFAULT_PRODUCTOS_CHUNK_SIZE,
    productos_tmp_dir: Path | None = None,
) -> ComercioOutcome:
    try:
        csvs = read_comercio_csvs(daily_zip_path, entry)
    except (zipfile.BadZipFile, KeyError) as exc:
        return _empty_outcome(entry, f"{type(exc).__name__}: {exc}")

    comercio_df, fecha_archivo = parse_comercio_csv(csvs["comercio.csv"])
    sucursales_df = parse_sucursales_csv(csvs["sucursales.csv"])

    if is_inactive(fecha_archivo, partition_date):
        rows_read = _count_productos_rows(csvs["productos.csv"], chunk_size_rows)
        lag_days = (partition_date - fecha_archivo).days
        outcome = ComercioOutcome(
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
            inactive_info=InactiveComercio(entry.comercio_id, entry.entry_name, fecha_archivo, lag_days),
        )
        return outcome

    non_productos_errors = validate_comercio_tables(
        comercio_df=comercio_df,
        sucursales_df=sucursales_df,
        fecha_archivo=fecha_archivo,
        partition_date=partition_date,
        max_lag_days=max_lag_days,
    )
    if non_productos_errors:
        rows_read = _count_productos_rows(csvs["productos.csv"], chunk_size_rows)
        return _empty_outcome(entry, "; ".join(non_productos_errors), rows_read=rows_read)

    # A partir de acá sí procesamos productos.csv de verdad, chunk por
    # chunk. Cada chunk limpio se escribe de una a un Parquet propio en un
    # scratch dir del comercio - nunca se acumulan los chunks en memoria,
    # y no se escribe nada al directorio final hasta decidir si el
    # comercio pasa (ver docstring del módulo).
    fecha_archivo_str = fecha_archivo.isoformat() if fecha_archivo else None
    scratch_dir = Path(tempfile.mkdtemp(prefix=f".scratch-{entry.comercio_id}-", dir=productos_tmp_dir))

    rows_read = 0
    rows_discarded = 0
    rows_corrected = 0
    discarded_rows_log: list[dict] = []
    chunk_paths: list[Path] = []

    for i, chunk in enumerate(
        parse_productos_csv_chunks(csvs["productos.csv"], chunk_size_rows=chunk_size_rows)
    ):
        chunk = chunk.assign(fecha_archivo=fecha_archivo_str)
        rows_read += len(chunk)

        row_outcome = apply_row_level_price_rules(chunk)
        rows_discarded += len(row_outcome.discarded_rows)
        rows_corrected += row_outcome.rows_corrected
        discarded_rows_log.extend(row_outcome.discarded_rows)

        if len(row_outcome.cleaned_df):
            chunk_path = scratch_dir / f"chunk-{i:04d}.parquet"
            row_outcome.cleaned_df.to_parquet(chunk_path, index=False)
            chunk_paths.append(chunk_path)

    if rows_read == 0:
        shutil.rmtree(scratch_dir, ignore_errors=True)
        return _empty_outcome(entry, "productos.csv no tiene filas")

    bad_ratio = (rows_discarded + rows_corrected) / rows_read

    if bad_ratio > max_bad_row_ratio:
        shutil.rmtree(scratch_dir, ignore_errors=True)
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
        rows_accepted=rows_read - rows_discarded,
        rows_discarded=rows_discarded,
        rows_corrected=rows_corrected,
        bad_ratio=bad_ratio,
        comercio_df=comercio_df,
        sucursales_df=sucursales_df,
        productos_chunk_paths=chunk_paths,
        productos_scratch_dir=scratch_dir,
        discarded_rows_log=discarded_rows_log,
    )


def _open_tmp_partition(table_dir: Path) -> Path:
    table_dir.mkdir(parents=True, exist_ok=True)
    return Path(tempfile.mkdtemp(prefix=".tmp-", dir=table_dir))


def _swap_in_partition(tmp_dir: Path, table_dir: Path, partition_date: date) -> None:
    final_dir = table_dir / f"dt={partition_date.isoformat()}"
    if not any(tmp_dir.iterdir()):
        # Ningún comercio aportó filas a esta tabla (p.ej. ninguno pasó) -
        # igual dejamos un Parquet vacío en vez de un directorio sin
        # archivos, para que read_parquet() sobre la partición no rompa.
        pd.DataFrame().to_parquet(tmp_dir / "part-00000.parquet", index=False)

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
    chunk_size_rows: int = DEFAULT_PRODUCTOS_CHUNK_SIZE,
) -> StageResult:
    result = StageResult(partition_date=partition_date)
    entries = list_comercio_entries(daily_zip_path)
    row_quarantine_entries: list[dict] = []

    # Un directorio temporal por tabla, abierto desde el arranque. Para
    # comercio/sucursales (chicas) cada comercio aceptado escribe su
    # propio archivo completo ahí mismo. Para productos, cada comercio ya
    # trae sus archivos por chunk ya escritos en un scratch dir (ver
    # _process_comercio) - acá solo se renombran al lugar final, sin leer
    # ni recomponer nada en memoria.
    tmp_dirs = {name: _open_tmp_partition(staging_dir / name) for name in TABLES}
    rows_written = {name: 0 for name in TABLES}

    for entry in entries:
        outcome = _process_comercio(
            daily_zip_path,
            entry,
            partition_date,
            max_bad_row_ratio,
            max_lag_days,
            chunk_size_rows,
            productos_tmp_dir=tmp_dirs["productos"],
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

        result.ok_comercio_ids.append(outcome.comercio_id)
        part_name = f"part-{outcome.comercio_id:05d}.parquet"

        outcome.comercio_df.to_parquet(tmp_dirs["comercio"] / part_name, index=False)
        rows_written["comercio"] += len(outcome.comercio_df)

        outcome.sucursales_df.to_parquet(tmp_dirs["sucursales"] / part_name, index=False)
        rows_written["sucursales"] += len(outcome.sucursales_df)

        for i, chunk_path in enumerate(outcome.productos_chunk_paths):
            final_chunk_path = tmp_dirs["productos"] / f"part-{outcome.comercio_id:05d}-{i:04d}.parquet"
            os.replace(chunk_path, final_chunk_path)
        rows_written["productos"] += outcome.rows_accepted
        if outcome.productos_scratch_dir is not None:
            # Ya se sacaron todos los chunks de ahí - lo que queda es el
            # directorio vacío. Si no se borra, termina adentro de la
            # partición final (tmp_dirs["productos"] se renombra entero).
            outcome.productos_scratch_dir.rmdir()

        for discard in outcome.discarded_rows_log:
            row_quarantine_entries.append({"comercio_id": outcome.comercio_id, **discard})

    for table_name in TABLES:
        _swap_in_partition(tmp_dirs[table_name], staging_dir / table_name, partition_date)
    result.rows_written = rows_written

    _write_quarantine_log(staging_dir, partition_date, result.quarantined)
    _write_row_quarantine_log(staging_dir, partition_date, row_quarantine_entries)
    _write_inactive_log(staging_dir, partition_date, result.inactive_comercios)
    _write_quality_report(staging_dir, partition_date, result.quality_report)

    return result
