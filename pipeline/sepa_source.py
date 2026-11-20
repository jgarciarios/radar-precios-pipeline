"""Resolución de la URL del ZIP diario del SEPA (datos.produccion.gob.ar).

La fuente no publica "el archivo de hoy": publica un archivo fijo por día
de la semana (sepa_lunes.zip ... sepa_domingo.zip) que el Estado
sobrescribe todos los días (ver dags/sepa_ingest_dag.py, fase 1, para el
porqué de catchup=False y de usar reloj en vez de logical_date).

Esta lógica vive en pipeline/, no en el DAG, para que tanto Airflow como
herramientas standalone (scripts/probe_sepa.py, corrido desde GitHub
Actions sin Airflow de por medio) resuelvan la URL contra la misma única
fuente de verdad - nunca copiando DAY_RESOURCES a mano en otro lado.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date as date_type
from datetime import datetime
from zoneinfo import ZoneInfo

TIMEZONE = "America/Argentina/Buenos_Aires"

DATASET_ID = "6f47ec76-d1ce-4e34-a7e1-621fe9b1d0b5"

# weekday() de Python: Monday=0 ... Sunday=6.
# IDs de recurso tomados de datos.produccion.gob.ar/dataset/sepa-precios.
# Si en el futuro empiezan a devolver 404, lo primero a revisar es si el
# dataset fue reestructurado y estos IDs de recurso cambiaron.
DAY_RESOURCES = {
    0: ("lunes", "0a9069a9-06e8-4f98-874d-da5578693290"),
    1: ("martes", "9dc06241-cc83-44f4-8e25-c9b1636b8bc8"),
    2: ("miercoles", "1e92cd42-4f94-4071-a165-62c4cb2ce23c"),
    3: ("jueves", "d076720f-a7f0-4af8-b1d6-1b99d5a90c14"),
    4: ("viernes", "91bc072a-4726-44a1-85ec-4a8467aad27e"),
    5: ("sabado", "b3c3da5d-213d-41e7-8d74-f23fda0a3c30"),
    6: ("domingo", "f8e75128-515a-436e-bf8d-5c63a62f2005"),
}


@dataclass(frozen=True)
class ResolvedSource:
    url: str
    day_name: str
    partition_date: str  # YYYY-MM-DD


def resolve_sepa_url(today: date_type) -> ResolvedSource:
    day_name, resource_id = DAY_RESOURCES[today.weekday()]
    url = (
        f"https://datos.produccion.gob.ar/dataset/{DATASET_ID}"
        f"/resource/{resource_id}/download/sepa_{day_name}.zip"
    )
    return ResolvedSource(url=url, day_name=day_name, partition_date=today.isoformat())


def resolve_sepa_url_now(tz: str = TIMEZONE) -> ResolvedSource:
    """Conveniencia para quien no tiene ya un "today" a mano (ej.
    scripts/probe_sepa.py) - usa zoneinfo de la stdlib en vez de pendulum,
    para no sumarle esa dependencia a un script standalone que solo
    necesita hacer un request HTTP."""
    today = datetime.now(ZoneInfo(tz)).date()
    return resolve_sepa_url(today)
