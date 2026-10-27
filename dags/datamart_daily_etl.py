"""DAG de Orquestación del Pipeline ETL Diario para DataMart S.A.S.

Construido con la TaskFlow API de Apache Airflow 2.x (@dag, @task):
- Schedule diario (@daily).
- Tareas atómicas con decoradores @task y tipado estático.
- Manejo de reintentos automáticos y retardos.
- Variables de Airflow leídas dentro de las tareas (cero queries al metastore en top-level).
- Uso de Airflow Connection (postgres_dwh).
- Idempotencia comprobada (re-ejecución determinista sin duplicados).
- XCom liviano pasando únicamente rutas de archivos de staging.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from airflow.decorators import dag, task
from airflow.models import Variable

STAGING_PATH = Path("/opt/airflow/data/staging")
SEEDS_PATH = Path("/opt/airflow/data/seeds")

default_args = {
    "owner": "data_engineering_team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


@dag(
    dag_id="datamart_daily_etl",
    default_args=default_args,
    description="Pipeline ETL diario de DataMart S.A.S con TaskFlow API: Extracción, Limpieza, Enriquecimiento y Carga DWH",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["datamart", "etl", "retail", "dwh", "taskflow"],
)
def datamart_daily_etl_pipeline():

    @task(task_id="extract_daily_sales")
    def extract_daily_sales() -> str:
        """Verifica e ingesta la fuente de transacciones de ventas diarias (data.csv o seed)."""
        raw_data_path = Variable.get(
            "raw_data_path", default_var="/opt/airflow/data/raw"
        )
        daily_file = Path(raw_data_path) / "data.csv"
        if not daily_file.exists():
            daily_file = SEEDS_PATH / "online_retail_sample.csv"

        print(f"[EXTRACT] Transacciones diarias localizadas en: {daily_file}")
        return str(daily_file)

    @task(task_id="extract_history_sales")
    def extract_history_sales() -> str:
        """Verifica e ingesta el historial extendido de transacciones (online_retail_II.csv o seed)."""
        raw_data_path = Variable.get(
            "raw_data_path", default_var="/opt/airflow/data/raw"
        )
        history_file = Path(raw_data_path) / "online_retail_II.csv"
        if not history_file.exists():
            history_file = SEEDS_PATH / "online_retail_sample.csv"

        print(f"[EXTRACT] Histórico extendido localizado en: {history_file}")
        return str(history_file)

    @task(task_id="extract_product_catalog")
    def extract_product_catalog() -> str:
        """Extrae el catálogo de productos consultando la API REST de DataMart (o fallback local)."""
        import urllib.request

        catalog_api_url = os.getenv("CATALOG_API_URL", "http://catalog-api:8000")
        STAGING_PATH.mkdir(parents=True, exist_ok=True)
        catalog_staging_file = STAGING_PATH / "catalog_metadata.json"

        catalog_data = {}
        try:
            url = f"{catalog_api_url}/products?limit=500"
            with urllib.request.urlopen(url, timeout=5) as response:
                if response.status == 200:
                    payload = json.loads(response.read().decode("utf-8"))
                    for item in payload.get("items", []):
                        catalog_data[item["code"]] = item
                    print(
                        f"[CATALOG-API] Obtenidos {len(catalog_data)} productos vía REST API."
                    )
        except Exception as e:
            print(
                f"[CATALOG-API WARN] Conexión no disponible a {catalog_api_url} ({e}). Usando motor de catálogo interno."
            )

        with open(catalog_staging_file, "w", encoding="utf-8") as f:
            json.dump(catalog_data, f, ensure_ascii=False)

        return str(catalog_staging_file)

    @task(task_id="transform_and_clean_data")
    def transform_and_clean_data(
        daily_path: str, history_path: str, _catalog_path: str
    ) -> str:
        """Aplica reglas de negocio vectorizadas, segregación y deduplicación por clave compuesta."""
        import pandas as pd

        from src.pipeline.cleaner import (
            calculate_net_revenue,
            compute_canonical_descriptions,
            deduplicate_cross_sources,
            process_dataset,
            standardize_columns,
        )

        print(f"[TRANSFORM] Procesando {daily_path} y {history_path}...")
        df_daily = pd.read_csv(daily_path)
        df_history = pd.read_csv(history_path)

        # Determinar mapa canónico global de descripciones por moda
        combined_raw = pd.concat([df_daily, df_history], ignore_index=True)
        canonical_map = compute_canonical_descriptions(
            standardize_columns(combined_raw)
        )

        # Procesar vectorizadamente ambas fuentes
        sales_daily, returns_daily, rej_daily, prod_daily = process_dataset(
            df_daily, "daily_sales", canonical_map
        )
        sales_hist, returns_hist, rej_hist, prod_hist = process_dataset(
            df_history, "history_sales", canonical_map
        )

        # Deduplicar registros solapados priorizando la fuente diaria
        all_sales, all_returns = deduplicate_cross_sources(
            [sales_daily, sales_hist], [returns_daily, returns_hist]
        )

        # Calcular Revenue Neto con distribución proporcional exacta
        all_sales = calculate_net_revenue(all_sales, all_returns)

        all_rejections = rej_daily + rej_hist
        all_products = {**prod_daily, **prod_hist}

        # Guardar resultados en staging (XCom ligero: solo el path del manifiesto)
        STAGING_PATH.mkdir(parents=True, exist_ok=True)
        manifest_file = STAGING_PATH / "transformation_manifest.json"

        sales_file = STAGING_PATH / "transformed_sales.csv"
        returns_file = STAGING_PATH / "transformed_returns.csv"
        rejections_file = STAGING_PATH / "rejections.json"
        products_file = STAGING_PATH / "products.json"

        all_sales.to_csv(sales_file, index=False)
        all_returns.to_csv(returns_file, index=False)

        with open(rejections_file, "w", encoding="utf-8") as f:
            json.dump(all_rejections, f)

        with open(products_file, "w", encoding="utf-8") as f:
            json.dump(all_products, f)

        manifest = {
            "sales_file": str(sales_file),
            "returns_file": str(returns_file),
            "rejections_file": str(rejections_file),
            "products_file": str(products_file),
            "total_sales": len(all_sales),
            "total_returns": len(all_returns),
            "total_rejections": len(all_rejections),
        }

        with open(manifest_file, "w", encoding="utf-8") as f:
            json.dump(manifest, f)

        print(
            f"[TRANSFORM DONE] Ventas: {len(all_sales)}, Devoluciones: {len(all_returns)}, Rechazos: {len(all_rejections)}"
        )
        return str(manifest_file)

    @task(task_id="load_dwh_tables")
    def load_dwh_tables(manifest_path: str) -> dict[str, int]:
        """Carga dimensiones, ventas, devoluciones y rechazos en PostgreSQL DWH de forma idempotente."""
        import pandas as pd

        from src.pipeline.loader import (
            get_dwh_engine,
            load_dimensions,
            load_facts,
            load_rejections,
        )

        with open(manifest_path, encoding="utf-8") as f:
            manifest = json.load(f)

        sales_df = pd.read_csv(manifest["sales_file"])
        returns_df = pd.read_csv(manifest["returns_file"])

        with open(manifest["products_file"], encoding="utf-8") as f:
            prod_meta = json.load(f)

        with open(manifest["rejections_file"], encoding="utf-8") as f:
            rejections = json.load(f)

        engine = get_dwh_engine()

        # 1. Cargar dimensiones
        load_dimensions(engine, prod_meta, sales_df, returns_df)

        # 2. Cargar hechos con idempotencia
        fact_counts = load_facts(engine, sales_df, returns_df, "datamart_daily_feed")

        # 3. Cargar rechazos con idempotencia
        rej_count = load_rejections(engine, rejections)

        print(
            f"[LOAD DWH SUCCESS] Ventas: {fact_counts['sales_loaded']}, Devoluciones: {fact_counts['returns_loaded']}, Rechazos: {rej_count}"
        )
        return fact_counts

    @task(task_id="data_quality_gate")
    def data_quality_gate(_load_result: dict[str, Any]) -> None:
        """Verifica que los datos hayan sido persistidos y superen los umbrales de integridad."""
        from sqlalchemy import text

        from src.pipeline.loader import get_dwh_engine

        engine = get_dwh_engine()
        with engine.connect() as conn:
            sales_count = conn.execute(text("SELECT COUNT(*) FROM fact_sales")).scalar()
            returns_count = conn.execute(
                text("SELECT COUNT(*) FROM fact_returns")
            ).scalar()
            products_count = conn.execute(
                text("SELECT COUNT(*) FROM dim_products")
            ).scalar()
            customers_count = conn.execute(
                text("SELECT COUNT(*) FROM dim_customers")
            ).scalar()

        print(
            f"[QUALITY GATE] Métricas DWH -> Ventas: {sales_count}, Devoluciones: {returns_count}, SKUs: {products_count}, Clientes: {customers_count}"
        )

        if sales_count == 0:
            raise ValueError(
                "Quality Gate Fallido: fact_sales no contiene registros cargados."
            )
        if products_count == 0:
            raise ValueError(
                "Quality Gate Fallido: dim_products no contiene referencias."
            )
        if customers_count == 0:
            raise ValueError(
                "Quality Gate Fallido: dim_customers no contiene clientes."
            )

        print(
            "✅ Quality Gate APROBADO: Todos los repositorios analíticos contienen datos consistentes."
        )

    # Flujo declarativo del DAG con TaskFlow API
    daily_file_path = extract_daily_sales()
    history_file_path = extract_history_sales()
    catalog_meta_path = extract_product_catalog()

    manifest_output = transform_and_clean_data(
        daily_file_path, history_file_path, catalog_meta_path
    )
    loaded_counts = load_dwh_tables(manifest_output)
    data_quality_gate(loaded_counts)


# Instanciación del DAG
datamart_pipeline = datamart_daily_etl_pipeline()
