"""Reglas de calidad, separadas en dos niveles:

- **Estructurales** (`validate_structural`): si fallan, el comercio entero
  va a cuarentena. Son cosas que invalidan TODO el archivo (no tiene filas,
  la fecha del footer no coincide con la partición).
- **Por fila** (`apply_row_level_price_rules`): se evalúan fila por fila
  dentro de productos.csv. Una fila con precio_lista nulo o negativo no
  sirve para nada (no hay precio que mostrar) y se descarta. Una fila con
  un precio secundario (referencia/promo) negativo sí tiene un precio
  principal válido, así que se conserva la fila y solo se anula ese campo
  puntual.

Antes estas dos cosas estaban mezcladas: cualquier precio nulo/negativo
tiraba el comercio entero a cuarentena. Eso significaba perder ~1M de
filas sanas de un comercio por 180 filas rotas (ver README, fase 2a). La
decisión de si el comercio entero es descartable ya no la toma esta
regla - la toma pipeline.stage comparando la proporción de filas
descartadas+corregidas contra un umbral configurable.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date

import pandas as pd

MANDATORY_PRICE_COLUMN = "productos_precio_lista"

# Si estuvieran ausentes o negativos no hay precio principal que mostrar,
# así que se tratan igual que un nulo: se descarta la fila.
OPTIONAL_PRICE_COLUMNS = (
    "productos_precio_referencia",
    "productos_precio_unitario_promo1",
    "productos_precio_unitario_promo2",
)


class ValidationError(Exception):
    pass


def _to_jsonable(value):
    if pd.isna(value):
        return None
    if hasattr(value, "item"):  # numpy scalar (float64, etc.)
        return value.item()
    return value


def _row_to_dict(row: pd.Series) -> dict:
    return {col: _to_jsonable(val) for col, val in row.items()}


@dataclass
class RowLevelOutcome:
    cleaned_df: pd.DataFrame
    discarded_rows: list[dict] = field(default_factory=list)  # [{"reason": ..., "row": {...}}, ...]
    rows_corrected: int = 0


def apply_row_level_price_rules(productos_df: pd.DataFrame) -> RowLevelOutcome:
    """Descarta filas sin precio principal usable y corrige (anula) precios
    secundarios negativos, fila por fila."""
    df = productos_df.copy()

    mandatory = df[MANDATORY_PRICE_COLUMN]
    discard_mask = mandatory.isna() | (mandatory < 0)

    discarded_rows = []
    for _, row in df[discard_mask].iterrows():
        reason = (
            f"{MANDATORY_PRICE_COLUMN} nulo"
            if pd.isna(row[MANDATORY_PRICE_COLUMN])
            else f"{MANDATORY_PRICE_COLUMN} negativo"
        )
        discarded_rows.append({"reason": reason, "row": _row_to_dict(row)})

    df = df[~discard_mask].copy()

    corrected_mask = pd.Series(False, index=df.index)
    for col in OPTIONAL_PRICE_COLUMNS:
        col_negative = df[col] < 0
        corrected_mask |= col_negative
        df.loc[col_negative, col] = pd.NA

    return RowLevelOutcome(
        cleaned_df=df,
        discarded_rows=discarded_rows,
        rows_corrected=int(corrected_mask.sum()),
    )


def validate_fecha_archivo(fecha_archivo: date | None, partition_date: date) -> list[str]:
    if fecha_archivo is None:
        return ["no se pudo extraer fecha_archivo del footer de comercio.csv"]
    if fecha_archivo != partition_date:
        return [f"fecha_archivo ({fecha_archivo}) distinta de la partición ({partition_date})"]
    return []


def validate_structural(
    *,
    comercio_df: pd.DataFrame,
    sucursales_df: pd.DataFrame,
    productos_df: pd.DataFrame,
    fecha_archivo: date | None,
    partition_date: date,
) -> list[str]:
    """Problemas que invalidan el comercio entero, sin mirar filas
    individuales de precio. Devuelve la lista de problemas (vacía si no hay
    ninguno) en vez de levantar una excepción, para que pipeline.stage
    decida qué hacer con cada caso."""
    errors: list[str] = []

    if len(comercio_df) == 0:
        errors.append("comercio.csv no tiene filas")
    if len(sucursales_df) == 0:
        errors.append("sucursales.csv no tiene filas")
    if len(productos_df) == 0:
        errors.append("productos.csv no tiene filas")

    errors += validate_fecha_archivo(fecha_archivo, partition_date)

    return errors
