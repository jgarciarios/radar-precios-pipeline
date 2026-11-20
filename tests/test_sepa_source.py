from datetime import date

from pipeline.sepa_source import DAY_RESOURCES, resolve_sepa_url, resolve_sepa_url_now


def test_resolve_sepa_url_builds_correct_url_for_each_weekday():
    # 2026-11-16 es lunes, cada +1 día es el siguiente día de la semana.
    for offset, (day_name, resource_id) in DAY_RESOURCES.items():
        today = date(2026, 11, 16 + offset)
        resolved = resolve_sepa_url(today)

        assert resolved.day_name == day_name
        assert resource_id in resolved.url
        assert resolved.url.endswith(f"sepa_{day_name}.zip")
        assert resolved.url.startswith("https://datos.produccion.gob.ar/dataset/")
        assert resolved.partition_date == today.isoformat()


def test_resolve_sepa_url_jueves_matches_known_real_url():
    # Confirmado contra la fuente real en la fase 1 (ver README).
    resolved = resolve_sepa_url(date(2026, 11, 19))  # jueves
    assert resolved.url == (
        "https://datos.produccion.gob.ar/dataset/6f47ec76-d1ce-4e34-a7e1-621fe9b1d0b5"
        "/resource/d076720f-a7f0-4af8-b1d6-1b99d5a90c14/download/sepa_jueves.zip"
    )


def test_resolve_sepa_url_now_returns_a_valid_resolved_source():
    resolved = resolve_sepa_url_now()
    assert resolved.day_name in {name for name, _ in DAY_RESOURCES.values()}
    assert resolved.url.startswith("https://datos.produccion.gob.ar/dataset/")
