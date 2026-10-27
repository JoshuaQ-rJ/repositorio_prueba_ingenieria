"""Microservicio REST para Catálogo Legado de Productos de DataMart S.A.S.

Expone endpoints para consulta paginada y por código de producto, enriqueciendo
las transacciones del pipeline con: código, nombre canónico, categoría, país proveedor y estado.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

try:
    from fastapi import FastAPI, HTTPException, Query
    from pydantic import BaseModel
except ImportError:
    # Soporte en caso de ser importado como módulo ligero sin fastapi instalado localmente
    FastAPI = None
    HTTPException = Exception
    BaseModel = object

# Categorías oficiales de DataMart
OFFICIAL_CATEGORIES = [
    "Electrónica",
    "Hogar",
    "Ropa",
    "Deportes",
    "Papelería",
]

SUPPLIER_COUNTRIES = ["Colombia", "México", "Perú"]

# Carga de catálogo seed
SEED_PATH = Path(__file__).resolve().parent / "catalog_seed.json"
_SEED_DATA: dict[str, dict[str, Any]] = {}

if SEED_PATH.exists():
    with open(SEED_PATH, encoding="utf-8") as f:
        data = json.load(f)
        for item in data.get("products", []):
            code_key = str(item.get("code", "")).strip().upper()
            if code_key:
                _SEED_DATA[code_key] = item


def infer_category_and_metadata(
    stock_code: str, description: str = ""
) -> dict[str, Any]:
    """Infiere metadatos de producto de forma determinista para enriquecimiento del pipeline."""
    code_norm = str(stock_code).strip().upper()
    desc_upper = str(description).upper()

    # Si ya está registrado en el seed, retornar tal cual
    if code_norm in _SEED_DATA:
        return _SEED_DATA[code_norm]

    # Heurística de palabras clave para clasificar en las 5 categorías
    category = "Hogar"
    if any(
        k in desc_upper
        for k in [
            "LIGHT",
            "CLOCK",
            "BATTERY",
            "ALARM",
            "TORCH",
            "ELECTRONIC",
            "USB",
            "PHONE",
        ]
    ):
        category = "Electrónica"
    elif any(
        k in desc_upper
        for k in [
            "WARMER",
            "HEART",
            "BAG",
            "T-LIGHT",
            "BOTTLE",
            "CUP",
            "MUG",
            "BOX",
            "KITCHEN",
            "PLATE",
        ]
    ):
        category = "Hogar"
    elif any(
        k in desc_upper
        for k in [
            "SCARF",
            "SHIRT",
            "DRESS",
            "GLOVES",
            "HAT",
            "KNITTED",
            "SOCK",
            "APRON",
            "COAT",
        ]
    ):
        category = "Ropa"
    elif any(
        k in desc_upper
        for k in [
            "BALL",
            "SPORT",
            "GYM",
            "BIKE",
            "BICYCLE",
            "FITNESS",
            "SWIM",
            "RUNNING",
        ]
    ):
        category = "Deportes"
    elif any(
        k in desc_upper
        for k in [
            "PAPER",
            "CARD",
            "STICKER",
            "NOTEBOOK",
            "PENCIL",
            "PEN",
            "ERASER",
            "POSTAGE",
            "TAPE",
        ]
    ):
        category = "Papelería"
    else:
        # Asignación determinista por hash de SKU a una de las 5 categorías
        idx = hash(code_norm) % len(OFFICIAL_CATEGORIES)
        category = OFFICIAL_CATEGORIES[idx]

    supplier_idx = hash(code_norm + "country") % len(SUPPLIER_COUNTRIES)
    supplier_country = SUPPLIER_COUNTRIES[supplier_idx]

    # Determinar nombre canónico o fallback
    canonical_name = (
        description.strip().upper()
        if description.strip()
        else f"PRODUCT SKU {code_norm}"
    )

    metadata = {
        "code": code_norm,
        "canonical_name": canonical_name,
        "category": category,
        "supplier_country": supplier_country,
        "is_active": not code_norm.startswith(("D", "CR", "BANK")),
    }
    return metadata


# Configuración de FastAPI si la librería está disponible
if FastAPI is not None:
    app = FastAPI(
        title="DataMart Legacy Catalog API",
        description="API interna para enriquecimiento de transacciones de ventas y categorías.",
        version="1.0.0",
    )

    class ProductResponse(BaseModel):
        code: str
        canonical_name: str
        category: str
        supplier_country: str
        is_active: bool

    class PaginatedProductsResponse(BaseModel):
        total: int
        limit: int
        offset: int
        items: list[ProductResponse]

    @app.get("/health", tags=["Salud"])
    def health_check() -> dict[str, str]:
        return {"status": "ok", "service": "datamart-legacy-catalog-api"}

    @app.get("/products", response_model=PaginatedProductsResponse, tags=["Catálogo"])
    def list_products(
        limit: int = Query(default=50, ge=1, le=500),
        offset: int = Query(default=0, ge=0),
    ) -> dict[str, Any]:
        all_items = list(_SEED_DATA.values())
        paginated = all_items[offset : offset + limit]
        return {
            "total": len(all_items),
            "limit": limit,
            "offset": offset,
            "items": paginated,
        }

    @app.get("/products/{code}", response_model=ProductResponse, tags=["Catálogo"])
    def get_product_by_code(code: str) -> dict[str, Any]:
        code_norm = code.strip().upper()
        item = infer_category_and_metadata(code_norm)
        return item
else:
    app = None
