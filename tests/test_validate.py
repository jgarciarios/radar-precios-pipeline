from datetime import date

import pandas as pd

from pipeline.validate import (
    apply_row_level_price_rules,
    is_inactive,
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


def test_validate_fecha_archivo_within_default_tolerance_of_1_day_passes():
    # Caso real: comercios 3/4/8/20, atraso de 1 día, default max_lag_days=1.
    assert validate_fecha_archivo(date(2026, 11, 18), date(2026, 11, 19)) == []


def test_validate_fecha_archivo_beyond_tolerance_is_reported():
    errors = validate_fecha_archivo(date(2026, 11, 10), date(2026, 11, 19), max_lag_days=1)
    assert "9 días de atraso" in errors[0]


def test_validate_fecha_archivo_respects_custom_max_lag_days():
    # con max_lag_days=0 no se tolera nada de atraso.
    errors = validate_fecha_archivo(date(2026, 11, 18), date(2026, 11, 19), max_lag_days=0)
    assert "1 días de atraso" in errors[0]
    assert validate_fecha_archivo(date(2026, 11, 19), date(2026, 11, 19), max_lag_days=0) == []


def test_validate_fecha_archivo_future_date_is_reported():
    errors = validate_fecha_archivo(date(2026, 11, 20), date(2026, 11, 19))
    assert "posterior a la partición" in errors[0]


# --- is_inactive -----------------------------------------------------------


def test_is_inactive_true_for_comercio_47_real_case():
    # comercio 47: sin actualizar desde 2025-06-11, partición 2026-11-19.
    assert is_inactive(date(2025, 6, 11), date(2026, 11, 19)) is True


def test_is_inactive_false_within_threshold():
    assert is_inactive(date(2026, 10, 1), date(2026, 11, 19)) is False  # ~49 días, no inactivo


def test_is_inactive_false_when_fecha_archivo_is_none():
    assert is_inactive(None, date(2026, 11, 19)) is False


def test_is_inactive_respects_custom_threshold():
    assert is_inactive(date(2026, 10, 1), date(2026, 11, 19), threshold_days=30) is True


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
        fecha_archivo=date(2026, 11, 10),  # 9 días, supera el max_lag_days default (1)
        partition_date=date(2026, 11, 19),
    )
    assert "sucursales.csv no tiene filas" in errors
    assert "productos.csv no tiene filas" in errors
    assert any("días de atraso" in e for e in errors)


def test_validate_structural_tolerates_lag_within_max_lag_days():
    errors = validate_structural(
        comercio_df=pd.DataFrame({"id_comercio": ["1"]}),
        sucursales_df=pd.DataFrame({"id_sucursal": ["100"]}),
        productos_df=_productos_df(),
        fecha_archivo=date(2026, 11, 18),
        partition_date=date(2026, 11, 19),
        max_lag_days=1,
    )
    assert errors == []
