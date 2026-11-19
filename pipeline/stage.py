"""Orquesta el pipeline raw -> staging para un día: desanida, parsea, valida
y escribe Parquet particionado por fecha.

Un comercio roto o que no pasa las reglas de calidad se registra en un log
de cuarentena y se excluye del resultado - no tira abajo el procesamiento
del resto. El resultado final (staging/<tabla>/dt=.../) se escribe de forma
atómica: se arma en un directorio temporal y recién al final se reemplaza
el directorio definitivo con os.rename (atómico en el mismo filesystem,
mismo patrón que la escritura del ZIP en la fase 1).

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
from pipeline.validate import ValidationError, validate_comercio

TABLES = ("comercio", "sucursales", "productos")


@dataclass
class QuarantinedComercio:
    comercio_id: int
    entry_name: str
    reason: str


@dataclass
class StageResult:
    partition_date: date
    rows_written: dict[str, int] = field(default_factory=dict)
    ok_comercio_ids: list[int] = field(default_factory=list)
    quarantined: list[QuarantinedComercio] = field(default_factory=list)


def find_daily_zip(raw_dir: Path, partition_date: date) -> Path:
    partition_dir = raw_dir / "sepa" / f"dt={partition_date.isoformat()}"
    matches = sorted(partition_dir.glob("*.zip"))
    if len(matches) != 1:
        raise FileNotFoundError(
            f"Esperaba exactamente 1 ZIP en {partition_dir}, encontré {len(matches)}: {matches}"
        )
    return matches[0]


def _process_comercio(
    daily_zip_path: Path, entry: ComercioEntry, partition_date: date
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Levanta cualquier excepción (BadZipFile, KeyError, ValidationError,
    errores de parseo de pandas) - _run la atrapa y manda a cuarentena."""
    csvs = read_comercio_csvs(daily_zip_path, entry)

    comercio_df, fecha_archivo = parse_comercio_csv(csvs["comercio.csv"])
    sucursales_df = parse_sucursales_csv(csvs["sucursales.csv"])
    productos_df = parse_productos_csv(csvs["productos.csv"])

    validate_comercio(
        comercio_df=comercio_df,
        sucursales_df=sucursales_df,
        productos_df=productos_df,
        fecha_archivo=fecha_archivo,
        partition_date=partition_date,
    )
    return comercio_df, sucursales_df, productos_df


def _atomic_write_partition(df: pd.DataFrame, table_dir: Path, partition_date: date) -> None:
    final_dir = table_dir / f"dt={partition_date.isoformat()}"
    table_dir.mkdir(parents=True, exist_ok=True)

    tmp_dir = Path(tempfile.mkdtemp(prefix=".tmp-", dir=table_dir))
    df.to_parquet(tmp_dir / "part-000.parquet", index=False)

    if final_dir.exists():
        shutil.rmtree(final_dir)
    os.rename(tmp_dir, final_dir)  # atómico: mismo filesystem, misma operación que en la fase 1


def _write_quarantine_log(
    staging_dir: Path, partition_date: date, quarantined: list[QuarantinedComercio]
) -> None:
    quarantine_dir = staging_dir / "_quarantine"
    quarantine_dir.mkdir(parents=True, exist_ok=True)
    final_path = quarantine_dir / f"dt={partition_date.isoformat()}.jsonl"

    fd, tmp_path_str = tempfile.mkstemp(prefix=".tmp-", dir=quarantine_dir, suffix=".jsonl")
    tmp_path = Path(tmp_path_str)
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        for q in quarantined:
            f.write(json.dumps({"comercio_id": q.comercio_id, "entry_name": q.entry_name, "reason": q.reason}) + "\n")

    os.replace(tmp_path, final_path)


def run_staging(daily_zip_path: Path, partition_date: date, staging_dir: Path) -> StageResult:
    result = StageResult(partition_date=partition_date)
    entries = list_comercio_entries(daily_zip_path)

    comercio_frames, sucursales_frames, productos_frames = [], [], []

    for entry in entries:
        try:
            comercio_df, sucursales_df, productos_df = _process_comercio(
                daily_zip_path, entry, partition_date
            )
        except (zipfile.BadZipFile, KeyError, ValidationError, ValueError) as exc:
            result.quarantined.append(
                QuarantinedComercio(
                    comercio_id=entry.comercio_id,
                    entry_name=entry.entry_name,
                    reason=f"{type(exc).__name__}: {exc}",
                )
            )
            continue

        comercio_frames.append(comercio_df)
        sucursales_frames.append(sucursales_df)
        productos_frames.append(productos_df)
        result.ok_comercio_ids.append(entry.comercio_id)

    tables = {
        "comercio": pd.concat(comercio_frames, ignore_index=True) if comercio_frames else pd.DataFrame(),
        "sucursales": pd.concat(sucursales_frames, ignore_index=True) if sucursales_frames else pd.DataFrame(),
        "productos": pd.concat(productos_frames, ignore_index=True) if productos_frames else pd.DataFrame(),
    }

    for table_name, df in tables.items():
        _atomic_write_partition(df, staging_dir / table_name, partition_date)
        result.rows_written[table_name] = len(df)

    _write_quarantine_log(staging_dir, partition_date, result.quarantined)

    return result
