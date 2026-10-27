"""Módulo de limpieza, estandarización y resolución de casos ambiguos para DataMart S.A.S.

Implementa las reglas de negocio de la sección 5 y la resolución de ambigüedades:
1. Separación de ventas y devoluciones (quantity <= 0).
2. Rechazo de registros con precio <= 0 al log de rechazos.
3. Estandarización de fechas a UTC.
4. Normalización de códigos de producto (mayúsculas y sin espacios).
5. Cálculo de gross_revenue y net_revenue agrupado diario por SKU.
6. Manejo de Customer ID nulo (-1 para compradores anónimos).
7. Resolución de descripción canónica por frecuencia máxima.
8. Deduplicación por clave compuesta entre datasets solapados.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

import pandas as pd

from src.api.main import infer_category_and_metadata


def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Homogeniza nombres de columnas dispares entre data.csv y online_retail_II.csv."""
    rename_map = {
        "Invoice": "invoice_no",
        "InvoiceNo": "invoice_no",
        "invoiceno": "invoice_no",
        "invoice": "invoice_no",
        "StockCode": "stock_code",
        "stockcode": "stock_code",
        "Description": "description",
        "description": "description",
        "Quantity": "quantity",
        "quantity": "quantity",
        "InvoiceDate": "invoice_date",
        "invoicedate": "invoice_date",
        "Price": "unit_price",
        "UnitPrice": "unit_price",
        "unitprice": "unit_price",
        "price": "unit_price",
        "Customer ID": "customer_id",
        "CustomerID": "customer_id",
        "customer_id": "customer_id",
        "customerid": "customer_id",
        "Country": "country",
        "country": "country",
    }
    df = df.rename(columns=rename_map).copy()
    return df


def compute_canonical_descriptions(df: pd.DataFrame) -> dict[str, str]:
    """Determina la descripción canónica por moda (más frecuente) para cada stock_code."""
    valid_desc = df.dropna(subset=["description"]).copy()
    valid_desc["stock_code"] = (
        valid_desc["stock_code"].astype(str).str.strip().str.upper()
    )
    valid_desc["description"] = (
        valid_desc["description"].astype(str).str.strip().str.upper()
    )

    # Filtramos descripciones vacías o códigos genéricos
    valid_desc = valid_desc[valid_desc["description"].str.len() > 1]

    # Conteo por SKU y descripción
    counts = (
        valid_desc.groupby(["stock_code", "description"])
        .size()
        .reset_index(name="freq")
    )
    # Ordenar por frecuencia descendente y tomar la primera por SKU
    canonical_df = counts.sort_values(
        ["stock_code", "freq"], ascending=[True, False]
    ).drop_duplicates(subset=["stock_code"], keep="first")

    return dict(zip(canonical_df["stock_code"], canonical_df["description"], strict=False))


def process_dataset(
    df: pd.DataFrame,
    source_name: str,
    canonical_map: dict[str, str] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, list[dict[str, Any]], dict[str, dict[str, Any]]]:
    """Procesa, limpia y segrega las transacciones en ventas válidas, devoluciones y rechazos.

    Retorna:
        - sales_df: DataFrame de ventas válidas
        - returns_df: DataFrame de devoluciones
        - rejections: Lista de diccionarios con registros rechazados
        - products_metadata: Diccionario de metadatos de producto enriquecidos
    """
    df = standardize_columns(df)

    if canonical_map is None:
        canonical_map = compute_canonical_descriptions(df)

    rejections: list[dict[str, Any]] = []
    sales_rows: list[dict[str, Any]] = []
    returns_rows: list[dict[str, Any]] = []
    products_metadata: dict[str, dict[str, Any]] = {}

    for idx, row in df.iterrows():
        raw_id = f"{source_name}_{idx}"
        invoice_no = str(row.get("invoice_no", "")).strip()
        raw_code = str(row.get("stock_code", "")).strip().upper()
        raw_desc = (
            str(row.get("description", "")).strip()
            if pd.notna(row.get("description"))
            else ""
        )
        country = str(row.get("country", "Unknown")).strip() or "Unknown"

        # Validación 1: Código de producto o factura ausente
        if not raw_code or not invoice_no or raw_code == "NAN":
            rejections.append(
                {
                    "source_file": source_name,
                    "raw_record_id": raw_id,
                    "rejection_reason": "MISSING_INVOICE_OR_STOCK_CODE",
                    "rejected_at": datetime.now().isoformat(),
                    "payload": json.dumps(row.to_dict(), default=str),
                }
            )
            continue

        # Normalización de código y descripción canónica
        stock_code = raw_code
        canonical_desc = canonical_map.get(
            stock_code, raw_desc.upper() or f"ITEM {stock_code}"
        )

        # Enriquecer producto con metadatos de categoría y país proveedor
        if stock_code not in products_metadata:
            products_metadata[stock_code] = infer_category_and_metadata(
                stock_code, canonical_desc
            )

        # Conversión de valores numéricos
        try:
            quantity = int(row.get("quantity", 0))
        except (ValueError, TypeError):
            rejections.append(
                {
                    "source_file": source_name,
                    "raw_record_id": raw_id,
                    "rejection_reason": "INVALID_QUANTITY_FORMAT",
                    "rejected_at": datetime.now().isoformat(),
                    "payload": json.dumps(row.to_dict(), default=str),
                }
            )
            continue

        try:
            unit_price = float(row.get("unit_price", 0.0))
        except (ValueError, TypeError):
            rejections.append(
                {
                    "source_file": source_name,
                    "raw_record_id": raw_id,
                    "rejection_reason": "INVALID_PRICE_FORMAT",
                    "rejected_at": datetime.now().isoformat(),
                    "payload": json.dumps(row.to_dict(), default=str),
                }
            )
            continue

        # Estandarización de fecha a UTC
        try:
            dt = pd.to_datetime(row.get("invoice_date"), utc=True)
            invoice_date_utc = dt.isoformat()
            date_key = int(dt.strftime("%Y%m%d"))
        except Exception:
            rejections.append(
                {
                    "source_file": source_name,
                    "raw_record_id": raw_id,
                    "rejection_reason": "INVALID_DATETIME_FORMAT",
                    "rejected_at": datetime.now().isoformat(),
                    "payload": json.dumps(row.to_dict(), default=str),
                }
            )
            continue

        # Manejo de Customer ID (Resolución de ambigüedad: -1 para anónimos)
        raw_cust_id = row.get("customer_id")
        if pd.isna(raw_cust_id) or str(raw_cust_id).strip() in ("", "nan", "None", "-"):
            customer_id = -1
        else:
            try:
                customer_id = int(float(raw_cust_id))
            except (ValueError, TypeError):
                customer_id = -1

        # REGLA DE NEGOCIO: Precio <= 0 en ventas es INVÁLIDO
        if unit_price <= 0:
            rejections.append(
                {
                    "source_file": source_name,
                    "raw_record_id": raw_id,
                    "rejection_reason": f"INVALID_UNIT_PRICE: {unit_price} <= 0",
                    "rejected_at": datetime.now().isoformat(),
                    "payload": json.dumps(row.to_dict(), default=str),
                }
            )
            continue

        # REGLA DE NEGOCIO: Cantidad <= 0 es devolución/ajuste
        if quantity <= 0 or invoice_no.startswith("C"):
            abs_qty = abs(quantity) if quantity != 0 else 1
            return_amount = round(abs_qty * unit_price, 4)
            returns_rows.append(
                {
                    "invoice_no": invoice_no,
                    "stock_code": stock_code,
                    "customer_id": customer_id,
                    "quantity": -abs_qty,
                    "unit_price": unit_price,
                    "return_amount": return_amount,
                    "return_date_utc": invoice_date_utc,
                    "date_key": date_key,
                    "country": country,
                    "source_file": source_name,
                }
            )
        else:
            gross_revenue = round(quantity * unit_price, 4)
            sales_rows.append(
                {
                    "invoice_no": invoice_no,
                    "stock_code": stock_code,
                    "customer_id": customer_id,
                    "quantity": quantity,
                    "unit_price": unit_price,
                    "gross_revenue": gross_revenue,
                    "net_revenue": gross_revenue,  # Se ajustará posteriormente con devoluciones del mismo día
                    "invoice_date_utc": invoice_date_utc,
                    "date_key": date_key,
                    "country": country,
                    "source_file": source_name,
                }
            )

    sales_df = pd.DataFrame(sales_rows)
    returns_df = pd.DataFrame(returns_rows)

    return sales_df, returns_df, rejections, products_metadata


def deduplicate_cross_sources(
    sales_dfs: list[pd.DataFrame], returns_dfs: list[pd.DataFrame]
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Deduplica registros solapados entre fuentes mediante clave compuesta, priorizando la fuente diaria."""
    composite_key = ["invoice_no", "stock_code", "invoice_date_utc", "quantity"]

    all_sales = pd.concat(sales_dfs, ignore_index=True) if sales_dfs else pd.DataFrame()
    if not all_sales.empty:
        # Priorizar data.csv (fuente diaria) sobre online_retail_II.csv
        all_sales["priority"] = all_sales["source_file"].apply(
            lambda s: 1 if "data.csv" in s or "daily" in s else 2
        )
        all_sales = all_sales.sort_values(by="priority").drop_duplicates(
            subset=composite_key, keep="first"
        )
        all_sales = all_sales.drop(columns=["priority"])

    return_key = ["invoice_no", "stock_code", "return_date_utc", "quantity"]
    all_returns = (
        pd.concat(returns_dfs, ignore_index=True) if returns_dfs else pd.DataFrame()
    )
    if not all_returns.empty:
        all_returns["priority"] = all_returns["source_file"].apply(
            lambda s: 1 if "data.csv" in s or "daily" in s else 2
        )
        all_returns = all_returns.sort_values(by="priority").drop_duplicates(
            subset=return_key, keep="first"
        )
        all_returns = all_returns.drop(columns=["priority"])

    return all_sales, all_returns


def calculate_net_revenue(
    sales_df: pd.DataFrame, returns_df: pd.DataFrame
) -> pd.DataFrame:
    """Calcula el revenue neto descontando el monto de devoluciones del mismo producto y fecha diaria."""
    if sales_df.empty:
        return sales_df

    if returns_df.empty:
        sales_df["net_revenue"] = sales_df["gross_revenue"]
        return sales_df

    # Sumar devoluciones por stock_code y date_key
    returns_agg = (
        returns_df.groupby(["stock_code", "date_key"])["return_amount"]
        .sum()
        .reset_index()
        .rename(columns={"return_amount": "daily_return_amount"})
    )

    merged = sales_df.merge(returns_agg, on=["stock_code", "date_key"], how="left")
    merged["daily_return_amount"] = merged["daily_return_amount"].fillna(0.0)

    # Distribuir o ajustar el revenue neto (mínimo 0 a nivel de línea o con deducción proporcional)
    merged["net_revenue"] = (
        merged["gross_revenue"] - merged["daily_return_amount"]
    ).clip(lower=0.0)
    merged = merged.drop(columns=["daily_return_amount"])

    return merged
