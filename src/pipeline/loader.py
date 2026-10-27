"""Módulo de carga y persistencia idempotente hacia el Data Warehouse PostgreSQL.

Implementa:
- Upsert/inserción segura de dimensiones (dim_products, dim_customers, dim_dates).
- Carga por lotes (batch loading) de fact_sales y fact_returns.
- Registro en auditoría de rejections_log.
- Idempotencia: previene duplicados en re-ejecuciones limpiando o actualizando según la partición.
"""

from __future__ import annotations

import json
import os
from typing import Any

import pandas as pd
from sqlalchemy import create_engine, text


def get_dwh_engine():
    """Obtiene el motor SQLAlchemy para el Data Warehouse analítico.

    Prioriza AIRFLOW_CONN_POSTGRES_DWH; si no existe, construye desde variables estándar.
    """
    conn_uri = os.getenv("AIRFLOW_CONN_POSTGRES_DWH")
    if not conn_uri:
        user = os.getenv("POSTGRES_DWH_USER", "datamart_user")
        pwd = os.getenv("POSTGRES_DWH_PASSWORD", "datamart_secure_pass")
        host = os.getenv("POSTGRES_DWH_HOST", "postgres-dwh")
        port = os.getenv("POSTGRES_DWH_PORT", "5432")
        db = os.getenv("POSTGRES_DWH_DB", "datamart_dwh")
        conn_uri = f"postgresql://{user}:{pwd}@{host}:{port}/{db}"

    # Soporte para formato de Airflow postgres:// -> postgresql://
    if conn_uri.startswith("postgres://"):
        conn_uri = conn_uri.replace("postgres://", "postgresql://", 1)

    return create_engine(conn_uri, pool_pre_ping=True)


def load_dimensions(engine, products_metadata: dict[str, dict[str, Any]], *dataframes: pd.DataFrame) -> None:
    """Inserta o actualiza dimensiones dim_products y dim_customers de forma idempotente."""
    with engine.begin() as conn:
        # 1. Dim Products
        for stock_code, meta in products_metadata.items():
            name = meta.get("canonical_name", f"ITEM {stock_code}")
            cat = meta.get("category", "Uncategorized")
            country = meta.get("supplier_country", "Unknown")
            is_active = meta.get("is_active", True)
            conn.execute(
                text("""
                    INSERT INTO dim_products (stock_code, canonical_name, category, supplier_country, is_active)
                    VALUES (:code, :name, :cat, :country, :active)
                    ON CONFLICT (stock_code) DO UPDATE
                    SET canonical_name = EXCLUDED.canonical_name,
                        category = EXCLUDED.category,
                        supplier_country = EXCLUDED.supplier_country,
                        is_active = EXCLUDED.is_active;
                """),
                {"code": stock_code, "name": name, "cat": cat, "country": country, "active": is_active},
            )

        # 2. Dim Customers (combina todos los dataframes proporcionados: ventas y devoluciones)
        cust_frames = [df[["customer_id", "country"]] for df in dataframes if not df.empty and "customer_id" in df.columns]
        if cust_frames:
            all_custs = pd.concat(cust_frames, ignore_index=True)
            unique_custs = all_custs.drop_duplicates(subset=["customer_id"])
            for _, row in unique_custs.iterrows():
                cid = int(row["customer_id"])
                cust_country = str(row["country"]) if pd.notna(row["country"]) else "Unknown"
                is_anon = (cid == -1)
                conn.execute(
                    text("""
                        INSERT INTO dim_customers (customer_id, customer_code, country, is_anonymous)
                        VALUES (:cid, :code, :country, :anon)
                        ON CONFLICT (customer_id) DO NOTHING;
                    """),
                    {
                        "cid": cid,
                        "code": f"CUST-{cid}" if cid != -1 else "ANONYMOUS",
                        "country": cust_country,
                        "anon": is_anon,
                    },
                )


def load_facts(engine, sales_df: pd.DataFrame, returns_df: pd.DataFrame, source_file: str) -> dict[str, int]:
    """Carga hechos de ventas y devoluciones garantizando idempotencia por archivo de origen."""
    counts = {"sales_loaded": 0, "returns_loaded": 0}

    with engine.begin() as conn:
        # Idempotencia: Si ya existen registros del mismo archivo fuente, eliminarlos antes de reinsertar
        conn.execute(
            text("DELETE FROM fact_sales WHERE invoice_no IN (SELECT DISTINCT invoice_no FROM fact_sales WHERE country = :src)"),
            {"src": source_file},  # Opcional, o por clave de factura
        )

        # Inserción de ventas en lotes
        if not sales_df.empty:
            sales_records = sales_df.to_dict(orient="records")
            conn.execute(
                text("""
                    INSERT INTO fact_sales (
                        invoice_no, stock_code, customer_id, quantity, unit_price,
                        gross_revenue, net_revenue, invoice_date_utc, date_key, country
                    ) VALUES (
                        :invoice_no, :stock_code, :customer_id, :quantity, :unit_price,
                        :gross_revenue, :net_revenue, :invoice_date_utc, :date_key, :country
                    )
                    ON CONFLICT (invoice_no, stock_code, invoice_date_utc, quantity) DO NOTHING;
                """),
                sales_records,
            )
            counts["sales_loaded"] = len(sales_records)

        # Inserción de devoluciones en lotes
        if not returns_df.empty:
            returns_records = returns_df.to_dict(orient="records")
            conn.execute(
                text("""
                    INSERT INTO fact_returns (
                        invoice_no, stock_code, customer_id, quantity, unit_price,
                        return_amount, return_date_utc, date_key, country
                    ) VALUES (
                        :invoice_no, :stock_code, :customer_id, :quantity, :unit_price,
                        :return_amount, :return_date_utc, :date_key, :country
                    )
                    ON CONFLICT (invoice_no, stock_code, return_date_utc, quantity) DO NOTHING;
                """),
                returns_records,
            )
            counts["returns_loaded"] = len(returns_records)

    return counts


def load_rejections(engine, rejections: list[dict[str, Any]]) -> int:
    """Inserta registros rechazados en la tabla rejections_log."""
    if not rejections:
        return 0

    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO rejections_log (source_file, raw_record_id, rejection_reason, rejected_at, payload)
                VALUES (:source_file, :raw_record_id, :rejection_reason, :rejected_at, :payload::jsonb)
            """),
            rejections,
        )
    return len(rejections)
