"""Entrypoint principal del proyecto DataMart S.A.S.

Permite ejecutar pruebas del pipeline, linters y validaciones locales directamente con uv.
"""

from __future__ import annotations

import sys
from pathlib import Path


def main():
    print("=" * 70)
    print("DataMart S.A.S. — Pipeline ETL & Data Warehouse Platform")
    print("=" * 70)
    print("Python gestionado por uv (3.12 pinned)")
    print("Ubicación del proyecto:", Path(__file__).resolve().parent)
    print("\nComandos útiles:")
    print(
        "  uv run pytest                     # Ejecuta las pruebas unitarias e integración"
    )
    print("  uv run ruff check .               # Linter de código estricto")
    print("  uv run ruff format .              # Formateador de código PEP 8")
    print(
        "  docker compose up -d              # Levanta Airflow, PostgreSQL Dual y Catalog API"
    )
    print("=" * 70)


if __name__ == "__main__":
    main()
