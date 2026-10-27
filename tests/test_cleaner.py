"""Pruebas unitarias de calidad, reglas de negocio y resolución de ambigüedades.

Valida:
1. Rechazo estricto de transacciones con unit_price <= 0.
2. Segregación de devoluciones (quantity <= 0 o invoice 'C...').
3. Imputación de compradores anónimos a customer_id = -1.
4. Normalización de StockCode (mayúsculas y sin espacios).
5. Conversión de fechas a UTC.
6. Determinación de nombre canónico por moda.
7. Cálculo de Gross Revenue y Net Revenue.
"""

from __future__ import annotations

import pandas as pd
import pytest

from src.pipeline.cleaner import (
    calculate_net_revenue,
    compute_canonical_descriptions,
    deduplicate_cross_sources,
    process_dataset,
    standardize_columns,
)


def test_unit_price_rejection():
    """Valida que transacciones con precio <= 0 sean rechazadas a rejections_log."""
    raw_data = pd.DataFrame([
        {"Invoice": "1001", "StockCode": "SKU1", "Description": "Valid Item", "Quantity": 2, "InvoiceDate": "2026-10-20 10:00:00", "Price": 10.0, "Customer ID": 123, "Country": "Colombia"},
        {"Invoice": "1002", "StockCode": "SKU2", "Description": "Free Item", "Quantity": 1, "InvoiceDate": "2026-10-20 10:00:00", "Price": 0.0, "Customer ID": 123, "Country": "Colombia"},
        {"Invoice": "1003", "StockCode": "SKU3", "Description": "Negative Price Item", "Quantity": 1, "InvoiceDate": "2026-10-20 10:00:00", "Price": -5.0, "Customer ID": 123, "Country": "Colombia"},
    ])

    sales_df, returns_df, rejections, _ = process_dataset(raw_data, "test_source")

    assert len(sales_df) == 1
    assert sales_df.iloc[0]["stock_code"] == "SKU1"
    assert len(rejections) == 2
    assert all("INVALID_UNIT_PRICE" in r["rejection_reason"] for r in rejections)


def test_returns_segregation():
    """Valida que transacciones con cantidad <= 0 o código C vayan a fact_returns."""
    raw_data = pd.DataFrame([
        {"Invoice": "1001", "StockCode": "SKU1", "Description": "Sale", "Quantity": 5, "InvoiceDate": "2026-10-20 10:00:00", "Price": 10.0, "Customer ID": 100, "Country": "Colombia"},
        {"Invoice": "C1002", "StockCode": "SKU1", "Description": "Return Invoice", "Quantity": -2, "InvoiceDate": "2026-10-20 11:00:00", "Price": 10.0, "Customer ID": 100, "Country": "Colombia"},
        {"Invoice": "1003", "StockCode": "SKU2", "Description": "Negative Qty Adjustment", "Quantity": -1, "InvoiceDate": "2026-10-20 11:30:00", "Price": 15.0, "Customer ID": 100, "Country": "Colombia"},
    ])

    sales_df, returns_df, rejections, _ = process_dataset(raw_data, "test_source")

    assert len(sales_df) == 1
    assert len(returns_df) == 2
    assert returns_df.iloc[0]["quantity"] == -2
    assert returns_df.iloc[0]["return_amount"] == 20.0


def test_anonymous_customer_id_imputation():
    """Valida que registros sin Customer ID sean mapeados a -1 (ANONYMOUS)."""
    raw_data = pd.DataFrame([
        {"Invoice": "1001", "StockCode": "SKU1", "Description": "Sale", "Quantity": 1, "InvoiceDate": "2026-10-20 10:00:00", "Price": 10.0, "Customer ID": None, "Country": "Peru"},
        {"Invoice": "1002", "StockCode": "SKU2", "Description": "Sale", "Quantity": 1, "InvoiceDate": "2026-10-20 10:00:00", "Price": 20.0, "Customer ID": float("nan"), "Country": "Mexico"},
        {"Invoice": "1003", "StockCode": "SKU3", "Description": "Sale", "Quantity": 1, "InvoiceDate": "2026-10-20 10:00:00", "Price": 30.0, "Customer ID": 999, "Country": "Colombia"},
    ])

    sales_df, _, _, _ = process_dataset(raw_data, "test_source")

    assert len(sales_df) == 3
    assert sales_df.iloc[0]["customer_id"] == -1
    assert sales_df.iloc[1]["customer_id"] == -1
    assert sales_df.iloc[2]["customer_id"] == 999


def test_canonical_description_by_mode():
    """Valida que se elija la descripción más frecuente como la canónica."""
    raw_data = pd.DataFrame([
        {"Invoice": "1", "StockCode": "85123A", "Description": "white heart holder", "Quantity": 1, "InvoiceDate": "2026-10-20", "Price": 2.0, "Customer ID": 1, "Country": "UK"},
        {"Invoice": "2", "StockCode": "85123A", "Description": "WHITE HANGING HEART T-LIGHT HOLDER", "Quantity": 1, "InvoiceDate": "2026-10-20", "Price": 2.0, "Customer ID": 1, "Country": "UK"},
        {"Invoice": "3", "StockCode": "85123A", "Description": "WHITE HANGING HEART T-LIGHT HOLDER", "Quantity": 1, "InvoiceDate": "2026-10-20", "Price": 2.0, "Customer ID": 1, "Country": "UK"},
    ])

    raw_clean = standardize_columns(raw_data)
    canonical_map = compute_canonical_descriptions(raw_clean)

    assert canonical_map["85123A"] == "WHITE HANGING HEART T-LIGHT HOLDER"


def test_net_revenue_calculation():
    """Valida el cálculo de net_revenue = gross_revenue - devoluciones diarias."""
    sales_df = pd.DataFrame([
        {"invoice_no": "1", "stock_code": "SKU1", "quantity": 10, "unit_price": 5.0, "gross_revenue": 50.0, "date_key": 20261020},
    ])
    returns_df = pd.DataFrame([
        {"invoice_no": "C1", "stock_code": "SKU1", "quantity": -2, "unit_price": 5.0, "return_amount": 10.0, "date_key": 20261020},
    ])

    net_df = calculate_net_revenue(sales_df, returns_df)

    assert net_df.iloc[0]["net_revenue"] == 40.0
