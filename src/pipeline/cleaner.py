"""Módulo de limpieza, estandarización y resolución de casos ambiguos para DataMart S.A.S.

Implementa las reglas de negocio de forma completamente vectorizada:
1. Separación de ventas y devoluciones (quantity <= 0 o factura 'C...').
2. Rechazo de registros con precio <= 0 al log de rechazos.
3. Estandarización de fechas a UTC con timezone explícito.
4. Normalización de códigos de producto (mayúsculas y sin espacios).
5. Cálculo de gross_revenue y distribución proporcional exacta de net_revenue.
6. Manejo de Customer ID nulo (-1 para compradores anónimos).
7. Resolución de descripción canónica por moda estadística.
8. Deduplicación por clave compuesta entre datasets solapados con prioridad diaria.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
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
    return df.rename(columns=rename_map).copy()


def compute_canonical_descriptions(df: pd.DataFrame) -> dict[str, str]:
    """Determina la descripción canónica por moda (más frecuente) para cada stock_code."""
    if "description" not in df.columns or "stock_code" not in df.columns:
        return {}

    valid_desc = df.dropna(subset=["description"]).copy()
    valid_desc["stock_code"] = (
        valid_desc["stock_code"].astype(str).str.strip().str.upper()
    )
    valid_desc["description"] = (
        valid_desc["description"].astype(str).str.strip().str.upper()
    )

    valid_desc = valid_desc[valid_desc["description"].str.len() > 1]
    if valid_desc.empty:
        return {}

    counts = (
        valid_desc.groupby(["stock_code", "description"])
        .size()
        .reset_index(name="freq")
    )
    canonical_df = counts.sort_values(
        ["stock_code", "freq"], ascending=[True, False]
    ).drop_duplicates(subset=["stock_code"], keep="first")

    return dict(
        zip(canonical_df["stock_code"], canonical_df["description"], strict=False)
    )


def process_dataset(
    df: pd.DataFrame,
    source_name: str,
    canonical_map: dict[str, str] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, list[dict[str, Any]], dict[str, dict[str, Any]]]:
    """Procesa, limpia y segrega las transacciones de manera vectorizada y de alto rendimiento.

    Retorna:
        - sales_df: DataFrame de ventas válidas
        - returns_df: DataFrame de devoluciones
        - rejections: Lista de diccionarios con registros rechazados
        - products_metadata: Diccionario de metadatos de producto enriquecidos
    """
    df = standardize_columns(df)
    now_utc_str = datetime.now(UTC).isoformat()

    if canonical_map is None:
        canonical_map = compute_canonical_descriptions(df)

    rejections: list[dict[str, Any]] = []

    # 1. Normalización inicial de cadenas
    df["invoice_no"] = df["invoice_no"].fillna("").astype(str).str.strip()
    df["stock_code"] = df["stock_code"].fillna("").astype(str).str.strip().str.upper()
    df["country"] = (
        df["country"].fillna("Unknown").astype(str).str.strip().replace("", "Unknown")
    )
    df["raw_record_id"] = [f"{source_name}_{i}" for i in df.index]

    # 2. Detección vectorizada de nulos en identificadores obligatorios
    mask_invalid_id = (
        (df["invoice_no"] == "")
        | (df["stock_code"] == "")
        | (df["stock_code"] == "NAN")
        | (df["stock_code"] == "NONE")
    )
    if mask_invalid_id.any():
        for _, row in df[mask_invalid_id].iterrows():
            rejections.append(
                {
                    "source_file": source_name,
                    "raw_record_id": row["raw_record_id"],
                    "rejection_reason": "MISSING_INVOICE_OR_STOCK_CODE",
                    "rejected_at": now_utc_str,
                    "payload": json.dumps(row.to_dict(), default=str),
                }
            )

    valid_df = df[~mask_invalid_id].copy()

    # 3. Conversión numérica vectorizada
    valid_df["quantity_num"] = pd.to_numeric(valid_df["quantity"], errors="coerce")
    valid_df["unit_price_num"] = pd.to_numeric(valid_df["unit_price"], errors="coerce")
    valid_df["dt_utc"] = pd.to_datetime(
        valid_df["invoice_date"], utc=True, errors="coerce"
    )

    # Rechazos por tipo numérico inválido
    mask_bad_qty = valid_df["quantity_num"].isna()
    if mask_bad_qty.any():
        for _, row in valid_df[mask_bad_qty].iterrows():
            rejections.append(
                {
                    "source_file": source_name,
                    "raw_record_id": row["raw_record_id"],
                    "rejection_reason": "INVALID_QUANTITY_FORMAT",
                    "rejected_at": now_utc_str,
                    "payload": json.dumps(row.to_dict(), default=str),
                }
            )

    mask_bad_price = valid_df["unit_price_num"].isna()
    if mask_bad_price.any():
        for _, row in valid_df[mask_bad_price].iterrows():
            rejections.append(
                {
                    "source_file": source_name,
                    "raw_record_id": row["raw_record_id"],
                    "rejection_reason": "INVALID_PRICE_FORMAT",
                    "rejected_at": now_utc_str,
                    "payload": json.dumps(row.to_dict(), default=str),
                }
            )

    mask_bad_dt = valid_df["dt_utc"].isna()
    if mask_bad_dt.any():
        for _, row in valid_df[mask_bad_dt].iterrows():
            rejections.append(
                {
                    "source_file": source_name,
                    "raw_record_id": row["raw_record_id"],
                    "rejection_reason": "INVALID_DATETIME_FORMAT",
                    "rejected_at": now_utc_str,
                    "payload": json.dumps(row.to_dict(), default=str),
                }
            )

    # Filtrar solo filas con formatos válidos
    clean_df = valid_df[~mask_bad_qty & ~mask_bad_price & ~mask_bad_dt].copy()

    # 4. REGLA DE NEGOCIO: Rechazo estricto si unit_price <= 0
    mask_price_zero_neg = clean_df["unit_price_num"] <= 0
    if mask_price_zero_neg.any():
        for _, row in clean_df[mask_price_zero_neg].iterrows():
            rejections.append(
                {
                    "source_file": source_name,
                    "raw_record_id": row["raw_record_id"],
                    "rejection_reason": f"INVALID_UNIT_PRICE: {row['unit_price_num']} <= 0",
                    "rejected_at": now_utc_str,
                    "payload": json.dumps(row.to_dict(), default=str),
                }
            )

    operational_df = clean_df[~mask_price_zero_neg].copy()

    # 5. Imputación vectorizada de Customer ID (Ambigüedad: anónimos = -1)
    operational_df["customer_id_clean"] = (
        pd.to_numeric(operational_df["customer_id"], errors="coerce")
        .fillna(-1)
        .astype(int)
    )

    # Fechas UTC estandarizadas y date_key
    operational_df["invoice_date_utc"] = operational_df["dt_utc"].dt.strftime(
        "%Y-%m-%dT%H:%M:%S+00:00"
    )
    operational_df["date_key"] = (
        operational_df["dt_utc"].dt.strftime("%Y%m%d").astype(int)
    )

    # 6. Segregación vectorizada: Devoluciones vs. Ventas
    mask_returns = (operational_df["quantity_num"] <= 0) | operational_df[
        "invoice_no"
    ].str.startswith("C")

    # --- Devoluciones ---
    returns_raw = operational_df[mask_returns].copy()
    if not returns_raw.empty:
        returns_raw["abs_qty"] = returns_raw["quantity_num"].abs().replace(0, 1)
        returns_raw["quantity"] = -returns_raw["abs_qty"].astype(int)
        returns_raw["unit_price"] = returns_raw["unit_price_num"].round(4)
        returns_raw["return_amount"] = (
            returns_raw["abs_qty"] * returns_raw["unit_price"]
        ).round(4)
        returns_raw["customer_id"] = returns_raw["customer_id_clean"]
        returns_raw["source_file"] = source_name

        returns_df = (
            returns_raw[
                [
                    "invoice_no",
                    "stock_code",
                    "customer_id",
                    "quantity",
                    "unit_price",
                    "return_amount",
                    "return_date_utc"
                    if "return_date_utc" in returns_raw
                    else "invoice_date_utc",
                    "date_key",
                    "country",
                    "source_file",
                ]
            ]
            .rename(columns={"invoice_date_utc": "return_date_utc"})
            .copy()
        )
    else:
        returns_df = pd.DataFrame(
            columns=[
                "invoice_no",
                "stock_code",
                "customer_id",
                "quantity",
                "unit_price",
                "return_amount",
                "return_date_utc",
                "date_key",
                "country",
                "source_file",
            ]
        )

    # --- Ventas ---
    sales_raw = operational_df[~mask_returns].copy()
    if not sales_raw.empty:
        sales_raw["quantity"] = sales_raw["quantity_num"].astype(int)
        sales_raw["unit_price"] = sales_raw["unit_price_num"].round(4)
        sales_raw["gross_revenue"] = (
            sales_raw["quantity"] * sales_raw["unit_price"]
        ).round(4)
        sales_raw["net_revenue"] = sales_raw["gross_revenue"]
        sales_raw["customer_id"] = sales_raw["customer_id_clean"]
        sales_raw["source_file"] = source_name

        sales_df = sales_raw[
            [
                "invoice_no",
                "stock_code",
                "customer_id",
                "quantity",
                "unit_price",
                "gross_revenue",
                "net_revenue",
                "invoice_date_utc",
                "date_key",
                "country",
                "source_file",
            ]
        ].copy()
    else:
        sales_df = pd.DataFrame(
            columns=[
                "invoice_no",
                "stock_code",
                "customer_id",
                "quantity",
                "unit_price",
                "gross_revenue",
                "net_revenue",
                "invoice_date_utc",
                "date_key",
                "country",
                "source_file",
            ]
        )

    # 7. Construcción de metadatos de producto
    unique_skus = operational_df["stock_code"].unique()
    products_metadata: dict[str, dict[str, Any]] = {}
    for sku in unique_skus:
        desc = canonical_map.get(sku, f"ITEM {sku}")
        products_metadata[sku] = infer_category_and_metadata(sku, desc)

    return sales_df, returns_df, rejections, products_metadata


def deduplicate_cross_sources(
    sales_dfs: list[pd.DataFrame],
    returns_dfs: list[pd.DataFrame],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Deduplica registros solapados entre fuentes mediante clave compuesta, priorizando la fuente diaria."""
    composite_key = ["invoice_no", "stock_code", "invoice_date_utc", "quantity"]

    valid_sales = [df for df in sales_dfs if not df.empty]
    if valid_sales:
        all_sales = pd.concat(valid_sales, ignore_index=True)
        # Prioridad 1 para diario/data.csv, prioridad 2 para histórico
        all_sales["priority"] = all_sales["source_file"].apply(
            lambda s: 1 if "data.csv" in s or "daily" in s else 2
        )
        all_sales = all_sales.sort_values(by="priority").drop_duplicates(
            subset=composite_key, keep="first"
        )
        all_sales = all_sales.drop(columns=["priority"])
    else:
        all_sales = pd.DataFrame()

    return_key = ["invoice_no", "stock_code", "return_date_utc", "quantity"]
    valid_returns = [df for df in returns_dfs if not df.empty]
    if valid_returns:
        all_returns = pd.concat(valid_returns, ignore_index=True)
        all_returns["priority"] = all_returns["source_file"].apply(
            lambda s: 1 if "data.csv" in s or "daily" in s else 2
        )
        all_returns = all_returns.sort_values(by="priority").drop_duplicates(
            subset=return_key, keep="first"
        )
        all_returns = all_returns.drop(columns=["priority"])
    else:
        all_returns = pd.DataFrame()

    return all_sales, all_returns


def calculate_net_revenue(
    sales_df: pd.DataFrame, returns_df: pd.DataFrame
) -> pd.DataFrame:
    """Calcula el revenue neto distribuyendo proporcionalmente el monto de devoluciones por SKU y día."""
    if sales_df.empty:
        return sales_df

    if returns_df.empty:
        sales_df["net_revenue"] = sales_df["gross_revenue"]
        return sales_df

    # Sumar devoluciones del mismo producto y fecha diaria
    returns_agg = (
        returns_df.groupby(["stock_code", "date_key"])["return_amount"]
        .sum()
        .reset_index()
        .rename(columns={"return_amount": "daily_return_amount"})
    )

    merged = sales_df.merge(returns_agg, on=["stock_code", "date_key"], how="left")
    merged["daily_return_amount"] = merged["daily_return_amount"].fillna(0.0)

    # Ponderación proporcional exacta: el descuento diario se distribuye según el peso del gross_revenue de cada línea
    group_gross = merged.groupby(["stock_code", "date_key"])["gross_revenue"].transform(
        "sum"
    )
    merged["weight"] = merged["gross_revenue"] / group_gross.replace(0, 1.0)
    merged["net_revenue"] = (
        (merged["gross_revenue"] - (merged["daily_return_amount"] * merged["weight"]))
        .clip(lower=0.0)
        .round(4)
    )

    merged = merged.drop(columns=["daily_return_amount", "weight"])
    return merged
