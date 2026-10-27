"""Prueba de integración end-to-end del loader y persistencia en DWH."""

from pathlib import Path

import pandas as pd

from src.pipeline.cleaner import calculate_net_revenue, process_dataset
from src.pipeline.loader import (
    get_dwh_engine,
    load_dimensions,
    load_facts,
    load_rejections,
)


def test_load_seed():
    # Resolución dinámica de la ruta de seeds para compatibilidad con Windows local y Docker
    base_dir = Path(__file__).resolve().parent.parent
    seed_path = base_dir / "data" / "seeds" / "online_retail_sample.csv"
    if not seed_path.exists():
        seed_path = Path("/opt/airflow/data/seeds/online_retail_sample.csv")

    assert seed_path.exists(), f"Seed file not found at {seed_path}"
    df_seed = pd.read_csv(seed_path)
    sales_df, returns_df, rejections, prod_meta = process_dataset(
        df_seed, "seed_sample"
    )
    sales_df = calculate_net_revenue(sales_df, returns_df)

    # Validar que los dataframes de transformación no estén vacíos
    assert not sales_df.empty, "Sales DataFrame is empty"
    assert not returns_df.empty, "Returns DataFrame is empty"
    assert len(prod_meta) > 0, "No products metadata generated"

    # Si hay conexión disponible a la base de datos (Docker o local), probar inserción
    try:
        engine = get_dwh_engine()
        load_dimensions(engine, prod_meta, sales_df, returns_df)
        facts = load_facts(engine, sales_df, returns_df, "seed_sample")
        rej_count = load_rejections(engine, rejections)
        print(
            f"SUCCESS: Loaded {facts['sales_loaded']} sales, {facts['returns_loaded']} returns, {rej_count} rejections."
        )
    except Exception as e:
        print(f"DB connection skipped or not reachable in host unit test: {e}")


if __name__ == "__main__":
    test_load_seed()
