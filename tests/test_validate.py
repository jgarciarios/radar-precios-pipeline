from datetime import date

import pandas as pd
import pytest

from pipeline.validate import ValidationError, validate_comercio, validate_fecha_archivo, validate_productos


def _productos_df(**overrides):
    base = {
        "productos_precio_lista": [100.0, 50.0],
        "productos_precio_referencia": [100.0, pd.NA],
        "productos_precio_unitario_promo1": [pd.NA, pd.NA],
        "productos_precio_unitario_promo2": [pd.NA, pd.NA],
    }
    base.update(overrides)
    return pd.DataFrame(base)


def test_validate_productos_ok_case_has_no_errors():
    assert validate_productos(_productos_df()) == []


def test_validate_productos_detects_empty_dataframe():
    empty_df = _productos_df().iloc[0:0]
    errors = validate_productos(empty_df)
    assert "no tiene filas" in errors[0]


def test_validate_productos_detects_null_precio_lista():
    errors = validate_productos(_productos_df(productos_precio_lista=[100.0, pd.NA]))
    assert any("precio_lista nulo" in e for e in errors)


def test_validate_productos_detects_negative_precio_lista():
    errors = validate_productos(_productos_df(productos_precio_lista=[100.0, -5.0]))
    assert any("precio_lista negativo" in e for e in errors)


def test_validate_productos_detects_negative_optional_price():
    errors = validate_productos(_productos_df(productos_precio_unitario_promo1=[-1.0, pd.NA]))
    assert any("productos_precio_unitario_promo1 negativo" in e for e in errors)


def test_validate_fecha_archivo_matches_partition():
    assert validate_fecha_archivo(date(2026, 11, 19), date(2026, 11, 19)) == []


def test_validate_fecha_archivo_mismatch_is_reported():
    errors = validate_fecha_archivo(date(2026, 11, 18), date(2026, 11, 19))
    assert "distinta de la partición" in errors[0]


def test_validate_fecha_archivo_missing_is_reported():
    errors = validate_fecha_archivo(None, date(2026, 11, 19))
    assert "no se pudo extraer" in errors[0]


def test_validate_comercio_raises_with_all_problems_combined():
    with pytest.raises(ValidationError) as exc_info:
        validate_comercio(
            comercio_df=pd.DataFrame({"id_comercio": ["1"]}),
            sucursales_df=pd.DataFrame({"id_sucursal": []}),
            productos_df=_productos_df(productos_precio_lista=[-5.0, 50.0]),
            fecha_archivo=date(2026, 11, 18),
            partition_date=date(2026, 11, 19),
        )
    message = str(exc_info.value)
    assert "sucursales.csv no tiene filas" in message
    assert "precio_lista negativo" in message
    assert "distinta de la partición" in message


def test_validate_comercio_ok_case_does_not_raise():
    validate_comercio(
        comercio_df=pd.DataFrame({"id_comercio": ["1"]}),
        sucursales_df=pd.DataFrame({"id_sucursal": ["100"]}),
        productos_df=_productos_df(),
        fecha_archivo=date(2026, 11, 19),
        partition_date=date(2026, 11, 19),
    )
