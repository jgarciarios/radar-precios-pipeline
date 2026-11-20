#!/usr/bin/env python3
"""Prueba de acceso al ZIP diario del SEPA, sin bajarlo entero.

Pensado para correr desde donde sea (ej. un runner de GitHub Actions sin
Airflow ni Docker de por medio) para confirmar que la fuente responde,
sin gastar los ~300MB del archivo real ni necesitar ninguna credencial -
es un endpoint HTTP público.

Pide con header Range los primeros RANGE_BYTES, pero además streamea la
respuesta y corta del lado del cliente al llegar a ese límite: si el
servidor ignorase el Range y mandara el archivo completo con código 200,
igual no se descarga más que RANGE_BYTES antes de cerrar la conexión.
"""
from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

import requests  # noqa: E402

from pipeline.sepa_source import resolve_sepa_url_now  # noqa: E402

RANGE_BYTES = 1024 * 1024  # 1 MB
CHUNK_SIZE = 64 * 1024


def main() -> int:
    resolved = resolve_sepa_url_now()
    print(f"url: {resolved.url}")
    print(f"día resuelto: {resolved.day_name} (partición {resolved.partition_date})")

    headers = {"Range": f"bytes=0-{RANGE_BYTES - 1}"}
    try:
        with requests.get(resolved.url, headers=headers, stream=True, timeout=(10, 30)) as response:
            received = 0
            for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                received += len(chunk)
                if received >= RANGE_BYTES:
                    break

            print(f"HTTP status: {response.status_code}")
            print(f"Content-Range: {response.headers.get('Content-Range')}")
            print(f"Content-Length (header): {response.headers.get('Content-Length')}")
            print(f"bytes recibidos (tope cliente {RANGE_BYTES}): {received}")
    except requests.RequestException as exc:
        print(f"ERROR de conexión: {exc}")
        return 1

    # 206 = Partial Content, el server respetó el Range. 200 también se
    # acepta como "accesible" (algún proxy puede ignorar el Range), lo
    # que importa para esta prueba es que respondió y mandó bytes reales.
    accessible = response.status_code in (200, 206) and received > 0
    print(f"accesible: {accessible}")
    return 0 if accessible else 1


if __name__ == "__main__":
    raise SystemExit(main())
