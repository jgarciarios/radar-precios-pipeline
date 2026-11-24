#!/usr/bin/env python3
"""Saca una captura del Graph de una corrida del DAG en la UI de Airflow,
para documentación (README). Herramienta de desarrollo, no del pipeline -
ver requirements-dev.txt.

Uso: python scripts/screenshot_dag_graph.py <dag_run_id> [salida.png]
"""
from __future__ import annotations

import sys
from pathlib import Path
from urllib.parse import quote

from playwright.sync_api import sync_playwright

BASE_URL = "http://localhost:8080"
USERNAME = "admin"
PASSWORD = "admin"
DAG_ID = "sepa_ingest_daily"


def main() -> int:
    if len(sys.argv) < 2:
        print("uso: screenshot_dag_graph.py <dag_run_id> [salida.png]")
        return 1

    dag_run_id = sys.argv[1]
    output_path = Path(sys.argv[2]) if len(sys.argv) > 2 else Path("docs/airflow_dag_graph.png")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    graph_url = f"{BASE_URL}/dags/{DAG_ID}/grid?dag_run_id={quote(dag_run_id)}&tab=graph"

    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page(viewport={"width": 1600, "height": 1000})

        page.goto(f"{BASE_URL}/login/")
        page.fill("#username", USERNAME)
        page.fill("#password", PASSWORD)
        page.click("input[type=submit]")
        page.wait_for_load_state("networkidle")

        page.goto(graph_url)
        page.wait_for_load_state("networkidle")
        # El grafo es un componente React que renderiza después del
        # networkidle inicial - esperamos a que aparezca al menos un nodo.
        page.wait_for_selector("[data-testid='node']", timeout=20000)
        page.wait_for_timeout(1000)  # deja asentar el layout/animación

        page.screenshot(path=str(output_path), full_page=True)
        browser.close()

    print(f"guardado en {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
