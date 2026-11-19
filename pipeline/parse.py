"""Parsea los 3 CSV que trae cada sub-ZIP de comercio.

Formato real observado en el ZIP del 2026-11-19 (ver README, sección
decisiones de diseño):
  - separador '|', UTF-8, fin de línea CRLF.
  - los 3 archivos terminan con una línea en blanco y una línea
    "Última actualización: <ISO 8601>" que NO es parte de la tabla - si no
    se filtra, pandas la lee como una fila con una sola columna rellena de
    NaN en el resto.
  - esa línea de footer es, de hecho, la fuente más confiable de "en qué
    fecha se generó este archivo" para la validación de freshness.
"""
from __future__ import annotations

import io
from datetime import date, datetime

import pandas as pd

FOOTER_PREFIX = "Última actualización:"

PRICE_COLUMNS = (
    "productos_precio_lista",
    "productos_precio_referencia",
    "productos_precio_unitario_promo1",
    "productos_precio_unitario_promo2",
)


def _decode(raw: bytes) -> str:
    """UTF-8 es lo que publica la fuente. Si algún comercio manda algo
    distinto, latin-1 nunca falla (mapea cualquier byte), así que lo usamos
    como último recurso en vez de tirar la fila entera del pipeline por un
    problema de encoding en un campo de texto libre (ej. descripción de
    producto)."""
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        return raw.decode("latin-1")


def _strip_footer(text: str) -> tuple[str, str | None]:
    lines = text.splitlines()
    footer_raw: str | None = None
    while lines and (lines[-1].strip() == "" or lines[-1].strip().startswith(FOOTER_PREFIX)):
        line = lines.pop().strip()
        if line.startswith(FOOTER_PREFIX):
            footer_raw = line[len(FOOTER_PREFIX):].strip()
    return "\n".join(lines), footer_raw


def _parse_footer_date(footer_raw: str | None) -> date | None:
    if not footer_raw:
        return None
    return datetime.fromisoformat(footer_raw).date()


def _read_table(raw: bytes) -> tuple[pd.DataFrame, date | None]:
    text = _decode(raw)
    clean_text, footer_raw = _strip_footer(text)
    df = pd.read_csv(io.StringIO(clean_text), sep="|", dtype=str, keep_default_na=False)
    return df, _parse_footer_date(footer_raw)


def parse_comercio_csv(raw: bytes) -> tuple[pd.DataFrame, date | None]:
    """Devuelve el DataFrame de comercio.csv y su fecha_archivo (del
    footer) - esta es la fecha que se compara contra la partición."""
    return _read_table(raw)


def parse_sucursales_csv(raw: bytes) -> pd.DataFrame:
    df, _ = _read_table(raw)
    return df


def parse_productos_csv(raw: bytes) -> pd.DataFrame:
    df, _ = _read_table(raw)
    for col in PRICE_COLUMNS:
        # '' (sin promo, por ejemplo) -> NaN, no un error de parseo.
        df[col] = pd.to_numeric(df[col].replace("", pd.NA), errors="coerce")
    return df
