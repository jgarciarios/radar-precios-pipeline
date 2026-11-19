"""Reglas de calidad explícitas, por comercio.

Un comercio que no pasa estas reglas se loguea y se excluye del staging de
hoy (ver pipeline.stage) - no tira abajo el procesamiento de los demás
comercios. Las reglas son funciones puras (DataFrame in, lista de errores
out) para poder testearlas sin tocar el filesystem ni el ZIP.
"""
from __future__ import annotations

from datetime import date

import pandas as pd

OPTIONAL_NON_NEGATIVE_PRICE_COLUMNS = (
    "productos_precio_referencia",
    "productos_precio_unitario_promo1",
    "productos_precio_unitario_promo2",
)


class ValidationError(Exception):
    pass


def validate_productos(productos_df: pd.DataFrame) -> list[str]:
    errors: list[str] = []

    if len(productos_df) == 0:
        return ["productos.csv no tiene filas"]

    n_nulos = productos_df["productos_precio_lista"].isna().sum()
    if n_nulos:
        errors.append(f"{n_nulos} filas con productos_precio_lista nulo")

    n_negativos = (productos_df["productos_precio_lista"].dropna() < 0).sum()
    if n_negativos:
        errors.append(f"{n_negativos} filas con productos_precio_lista negativo")

    # Los precios de referencia/promo pueden faltar (no todo producto tiene
    # promo), pero si están presentes no pueden ser negativos.
    for col in OPTIONAL_NON_NEGATIVE_PRICE_COLUMNS:
        n_negativos_col = (productos_df[col].dropna() < 0).sum()
        if n_negativos_col:
            errors.append(f"{n_negativos_col} filas con {col} negativo")

    return errors


def validate_fecha_archivo(fecha_archivo: date | None, partition_date: date) -> list[str]:
    if fecha_archivo is None:
        return ["no se pudo extraer fecha_archivo del footer de comercio.csv"]
    if fecha_archivo != partition_date:
        return [f"fecha_archivo ({fecha_archivo}) distinta de la partición ({partition_date})"]
    return []


def validate_comercio(
    *,
    comercio_df: pd.DataFrame,
    sucursales_df: pd.DataFrame,
    productos_df: pd.DataFrame,
    fecha_archivo: date | None,
    partition_date: date,
) -> None:
    """Levanta ValidationError con todos los problemas encontrados (no solo
    el primero), para que el log de cuarentena diga todo de una."""
    errors: list[str] = []

    if len(comercio_df) == 0:
        errors.append("comercio.csv no tiene filas")
    if len(sucursales_df) == 0:
        errors.append("sucursales.csv no tiene filas")

    errors += validate_productos(productos_df)
    errors += validate_fecha_archivo(fecha_archivo, partition_date)

    if errors:
        raise ValidationError("; ".join(errors))
