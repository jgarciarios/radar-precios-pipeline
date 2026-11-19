from datetime import date

import pandas as pd

from pipeline.validate import (
    apply_row_level_price_rules,
    validate_fecha_archivo,
    validate_structural,
)


def _productos_df(**overrides):
    base = {
        "productos_precio_lista": [100.0, 50.0],
        "productos_precio_referencia": [100.0, pd.NA],
        "productos_precio_unitario_promo1": [pd.NA, pd.NA],
        "productos_precio_unitario_promo2": [pd.NA, pd.NA],
    }
    base.update(overrides)
    return pd.DataFrame(base)


# --- apply_row_level_price_rules -----------------------------------------


def test_row_rules_ok_case_discards_and_corrects_nothing():
    outcome = apply_row_level_price_rules(_productos_df())
    assert len(outcome.cleaned_df) == 2
    assert outcome.discarded_rows == []
    assert outcome.rows_corrected == 0


def test_row_rules_discards_null_precio_lista():
    outcome = apply_row_level_price_rules(_productos_df(productos_precio_lista=[100.0, pd.NA]))
    assert len(outcome.cleaned_df) == 1
    assert len(outcome.discarded_rows) == 1
    assert outcome.discarded_rows[0]["reason"] == "productos_precio_lista nulo"


def test_row_rules_discards_negative_precio_lista():
    outcome = apply_row_level_price_rules(_productos_df(productos_precio_lista=[100.0, -5.0]))
    assert len(outcome.cleaned_df) == 1
    assert outcome.discarded_rows[0]["reason"] == "productos_precio_lista negativo"


def test_row_rules_discarded_row_includes_raw_fields():
    outcome = apply_row_level_price_rules(_productos_df(productos_precio_lista=[100.0, pd.NA]))
    raw = outcome.discarded_rows[0]["row"]
    assert raw["productos_precio_lista"] is None
    assert raw["productos_precio_referencia"] is None  # NA de la fixture


def test_row_rules_keeps_row_and_nulls_negative_optional_price():
    outcome = apply_row_level_price_rules(
        _productos_df(productos_precio_referencia=[100.0, -28447.37])
    )
    assert len(outcome.cleaned_df) == 2  # no se descarta ninguna fila
    assert outcome.discarded_rows == []
    assert outcome.rows_corrected == 1
    corrected_row = outcome.cleaned_df.iloc[1]
    assert pd.isna(corrected_row["productos_precio_referencia"])
    assert corrected_row["productos_precio_lista"] == 50.0  # el precio principal queda intacto


def test_row_rules_counts_one_corrected_row_even_with_two_bad_optional_fields():
    outcome = apply_row_level_price_rules(
        _productos_df(
            productos_precio_referencia=[100.0, -1.0],
            productos_precio_unitario_promo1=[pd.NA, -2.0],
        )
    )
    assert outcome.rows_corrected == 1  # misma fila, dos campos corregidos


# --- validate_fecha_archivo (sin cambios de comportamiento) ---------------


def test_validate_fecha_archivo_matches_partition():
    assert validate_fecha_archivo(date(2026, 11, 19), date(2026, 11, 19)) == []


def test_validate_fecha_archivo_mismatch_is_reported():
    errors = validate_fecha_archivo(date(2026, 11, 18), date(2026, 11, 19))
    assert "distinta de la partición" in errors[0]


# --- validate_structural ---------------------------------------------------


def test_validate_structural_ok_case_has_no_errors():
    errors = validate_structural(
        comercio_df=pd.DataFrame({"id_comercio": ["1"]}),
        sucursales_df=pd.DataFrame({"id_sucursal": ["100"]}),
        productos_df=_productos_df(),
        fecha_archivo=date(2026, 11, 19),
        partition_date=date(2026, 11, 19),
    )
    assert errors == []


def test_validate_structural_does_not_look_at_individual_prices():
    # precio_lista negativo en una fila ya NO es un problema estructural -
    # eso lo maneja apply_row_level_price_rules, no esta función.
    errors = validate_structural(
        comercio_df=pd.DataFrame({"id_comercio": ["1"]}),
        sucursales_df=pd.DataFrame({"id_sucursal": ["100"]}),
        productos_df=_productos_df(productos_precio_lista=[-5.0, 50.0]),
        fecha_archivo=date(2026, 11, 19),
        partition_date=date(2026, 11, 19),
    )
    assert errors == []


def test_validate_structural_detects_all_empty_tables_and_fecha_mismatch():
    errors = validate_structural(
        comercio_df=pd.DataFrame({"id_comercio": ["1"]}),
        sucursales_df=pd.DataFrame({"id_sucursal": []}),
        productos_df=_productos_df().iloc[0:0],
        fecha_archivo=date(2026, 11, 18),
        partition_date=date(2026, 11, 19),
    )
    assert "sucursales.csv no tiene filas" in errors
    assert "productos.csv no tiene filas" in errors
    assert any("distinta de la partición" in e for e in errors)
