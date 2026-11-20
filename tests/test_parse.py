from datetime import date

import pandas as pd

from pipeline.parse import parse_comercio_csv, parse_productos_csv, parse_sucursales_csv


def test_parse_comercio_csv_strips_footer_and_extracts_fecha():
    raw = (
        "id_comercio|comercio_razon_social\r\n"
        "24|Farmcity S.A.\r\n"
        "\r\n"
        "Última actualización: 2026-11-19T02:01:37-03:00\r\n"
    ).encode()

    df, fecha_archivo = parse_comercio_csv(raw)

    assert list(df["id_comercio"]) == ["24"]
    assert fecha_archivo == date(2026, 11, 19)


def test_parse_sucursales_csv_strips_footer():
    raw = (
        "id_comercio|id_sucursal\r\n"
        "24|194\r\n"
        "\r\n"
        "Última actualización: 2026-11-19T02:01:37-03:00\r\n"
    ).encode()

    df = parse_sucursales_csv(raw)

    assert len(df) == 1
    assert list(df.columns) == ["id_comercio", "id_sucursal"]


def test_parse_productos_csv_converts_prices_and_handles_empty_promo():
    raw = (
        "id_comercio|id_producto|productos_precio_lista|productos_precio_referencia|"
        "productos_precio_unitario_promo1|productos_leyenda_promo1|"
        "productos_precio_unitario_promo2|productos_leyenda_promo2\r\n"
        "24|1|100.5|100.5||| |\r\n"
        "\r\n"
        "Última actualización: 2026-11-19T02:01:43-03:00\r\n"
    ).encode()

    df = parse_productos_csv(raw)

    assert df.loc[0, "productos_precio_lista"] == 100.5
    assert pd.isna(df.loc[0, "productos_precio_unitario_promo1"])


def test_parse_handles_footer_variants_seen_in_real_data():
    # Variantes reales encontradas en el ZIP del 2026-11-19: con/sin tilde,
    # mayúscula distinta, \n en vez de \r\n, y una línea de puro byte NUL
    # antes del footer (comercio 47).
    variants = [
        "id_comercio|x\r\n1|a\r\n\r\nUltima actualizacion: 2026-11-19T01:11:08-03:00\r\n",
        "id_comercio|x\n1|a\n\nUltima actualización: 2026-11-19T08:30:02-03:00\n",
        "id_comercio|x\r\n1|a\r\n\r\nUltima Actualizacion: 2026-11-19T05:01:37-03:00\r\n",
        "id_comercio|x\n1|a\n\x00\nUltima actualizacion: 2025-06-11T21:00:01-03:00\n",
    ]
    for text in variants:
        df, fecha_archivo = parse_comercio_csv(text.encode("utf-8"))
        assert len(df) == 1, text
        assert fecha_archivo is not None, text


def test_parse_falls_back_to_latin1_on_bad_utf8():
    # 'ñ' en latin-1 (0xF1), bytes inválidos como UTF-8.
    raw = "id_comercio|comercio_razon_social\r\n1|Espa\xf1a S.A.\r\n".encode("latin-1")

    df, _ = parse_comercio_csv(raw)

    assert df.loc[0, "comercio_razon_social"] == "España S.A."


# --- parse_productos_csv_chunks: equivalencia contra el método anterior ---


def _build_productos_csv(n_rows: int) -> bytes:
    header = (
        "id_comercio|id_producto|productos_precio_lista|productos_precio_referencia|"
        "productos_precio_unitario_promo1|productos_precio_unitario_promo2\r\n"
    )
    rows = "".join(
        f"24|{i}|{100.0 + i}|{90.0 + i}||\r\n" for i in range(n_rows)
    )
    return (header + rows + "\r\nÚltima actualización: 2026-11-19T02:01:43-03:00\r\n").encode()


def test_parse_productos_csv_chunks_equals_non_chunked_method():
    from pipeline.parse import parse_productos_csv_chunks

    raw = _build_productos_csv(n_rows=10)

    whole = parse_productos_csv(raw)
    # chunk_size_rows=3 con 10 filas -> 4 chunks (3,3,3,1), para probar
    # que la frontera de un chunk no cambia nada.
    chunked = pd.concat(list(parse_productos_csv_chunks(raw, chunk_size_rows=3)), ignore_index=True)

    pd.testing.assert_frame_equal(whole, chunked)


def test_parse_productos_csv_chunks_yields_one_chunk_when_chunk_size_exceeds_rows():
    from pipeline.parse import parse_productos_csv_chunks

    raw = _build_productos_csv(n_rows=5)
    chunks = list(parse_productos_csv_chunks(raw, chunk_size_rows=1000))

    assert len(chunks) == 1
    assert len(chunks[0]) == 5


def test_parse_productos_csv_chunks_handles_empty_productos():
    from pipeline.parse import parse_productos_csv_chunks

    raw = _build_productos_csv(n_rows=0)
    chunks = list(parse_productos_csv_chunks(raw, chunk_size_rows=10))

    assert sum(len(c) for c in chunks) == 0
