"""Prueba de integración end-to-end del loader y persistencia en DWH."""

import pandas as pd
from src.pipeline.cleaner import process_dataset, calculate_net_revenue
from src.pipeline.loader import get_dwh_engine, load_dimensions, load_facts, load_rejections

def test_load_seed():
    df_seed = pd.read_csv('/opt/airflow/data/seeds/online_retail_sample.csv')
    sales_df, returns_df, rejections, prod_meta = process_dataset(df_seed, 'seed_sample')
    sales_df = calculate_net_revenue(sales_df, returns_df)

    engine = get_dwh_engine()
    load_dimensions(engine, prod_meta, sales_df, returns_df)
    facts = load_facts(engine, sales_df, returns_df, 'seed_sample')
    rej_count = load_rejections(engine, rejections)

    print(f"SUCCESS: Loaded {facts['sales_loaded']} sales, {facts['returns_loaded']} returns, {rej_count} rejections, {len(prod_meta)} products.")

if __name__ == "__main__":
    test_load_seed()
