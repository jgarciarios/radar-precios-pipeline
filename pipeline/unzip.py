"""Desanida el ZIP diario del SEPA: un sub-ZIP por comercio, cada uno con
comercio.csv, sucursales.csv y productos.csv adentro.

No hay garantía de que todos los sub-ZIPs estén sanos: en el ZIP del
2026-11-19 real, uno de los 18 sub-ZIPs pesaba 0 bytes. Por eso separamos
"listar las entradas" (no puede fallar por datos corruptos, solo lee los
nombres) de "leer el contenido de una entrada" (sí puede fallar, y el
llamador decide qué hacer con eso - ver pipeline.stage).
"""
from __future__ import annotations

import io
import re
import zipfile
from dataclasses import dataclass
from pathlib import Path

_COMERCIO_ID_RE = re.compile(r"comercio-sepa-(\d+)")

EXPECTED_CSVS = ("comercio.csv", "sucursales.csv", "productos.csv")


@dataclass(frozen=True)
class ComercioEntry:
    comercio_id: int
    entry_name: str  # nombre del sub-zip dentro del zip diario


def list_comercio_entries(daily_zip_path: Path) -> list[ComercioEntry]:
    entries = []
    with zipfile.ZipFile(daily_zip_path) as daily_zip:
        for name in daily_zip.namelist():
            if not name.endswith(".zip"):
                continue
            match = _COMERCIO_ID_RE.search(name)
            if not match:
                raise ValueError(f"No pude extraer el id de comercio de {name!r}")
            entries.append(ComercioEntry(comercio_id=int(match.group(1)), entry_name=name))
    return entries


def read_comercio_csvs(daily_zip_path: Path, entry: ComercioEntry) -> dict[str, bytes]:
    """Lee los 3 CSV de un comercio.

    Levanta zipfile.BadZipFile si el sub-zip está roto, o KeyError si le
    falta alguno de los 3 archivos esperados. A propósito no atrapamos esas
    excepciones acá: quien orquesta (pipeline.stage) decide si un comercio
    roto se loguea y se saltea o si debe frenar todo.
    """
    with zipfile.ZipFile(daily_zip_path) as daily_zip:
        raw_bytes = daily_zip.read(entry.entry_name)

    with zipfile.ZipFile(io.BytesIO(raw_bytes)) as comercio_zip:
        return {name: comercio_zip.read(name) for name in EXPECTED_CSVS}
