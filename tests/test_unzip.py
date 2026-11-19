import io
import zipfile

import pytest

from pipeline.unzip import list_comercio_entries, read_comercio_csvs


def _build_comercio_subzip(csv_contents: dict[str, bytes]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        for name, content in csv_contents.items():
            z.writestr(name, content)
    return buf.getvalue()


@pytest.fixture
def daily_zip_path(tmp_path):
    """Simula un ZIP diario con 2 comercios: uno sano y uno roto (0 bytes),
    como pasó de verdad con sepa_2_comercio-sepa-36 el 2026-11-19."""
    good_subzip = _build_comercio_subzip(
        {
            "comercio.csv": b"id_comercio|comercio_razon_social\r\n1|Comercio Sano S.A.\r\n",
            "sucursales.csv": b"id_comercio|id_sucursal\r\n1|100\r\n",
            "productos.csv": b"id_comercio|id_producto|productos_precio_lista\r\n1|1|100.0\r\n",
        }
    )

    daily_zip_path = tmp_path / "sepa_jueves.zip"
    with zipfile.ZipFile(daily_zip_path, "w") as daily_zip:
        daily_zip.writestr("2026-11-19/sepa_1_comercio-sepa-1_2026-11-19_09-05-10.zip", good_subzip)
        daily_zip.writestr("2026-11-19/sepa_2_comercio-sepa-36_2026-11-19_01-05-08.zip", b"")

    return daily_zip_path


def test_list_comercio_entries_finds_both_entries(daily_zip_path):
    entries = list_comercio_entries(daily_zip_path)
    ids = sorted(e.comercio_id for e in entries)
    assert ids == [1, 36]


def test_read_comercio_csvs_returns_the_three_files(daily_zip_path):
    entries = {e.comercio_id: e for e in list_comercio_entries(daily_zip_path)}
    csvs = read_comercio_csvs(daily_zip_path, entries[1])
    assert set(csvs) == {"comercio.csv", "sucursales.csv", "productos.csv"}
    assert b"Comercio Sano S.A." in csvs["comercio.csv"]


def test_read_comercio_csvs_raises_on_broken_subzip(daily_zip_path):
    entries = {e.comercio_id: e for e in list_comercio_entries(daily_zip_path)}
    with pytest.raises(zipfile.BadZipFile):
        read_comercio_csvs(daily_zip_path, entries[36])
