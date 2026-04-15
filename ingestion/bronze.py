"""
ingestion/bronze.py
-------------------
ingestion layer — incremental ingestion of all 7 Olist CSVs into PostgreSQL.

- orders:      watermark-based (only new rows since last run)
- all others:  full replace (static/reference tables)

All tables get _ingested_at audit column.
All runs logged to pipeline_run_log.
"""
import os
import pandas as pd
from utils.db import get_engine
from utils.watermark import get_last_watermark, log_run_start, log_run_end

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "raw")
engine = get_engine()


def _csv(filename):
    return os.path.join(DATA_DIR, filename)


# ------------------------------------------------------------------
# ORDERS  — watermark on order_purchase_timestamp
# ------------------------------------------------------------------
def ingest_orders():
    table = "bronze_orders"
    run_id = log_run_start(table)
    try:
        last_wm = get_last_watermark(table)
        df = pd.read_csv(
            _csv("olist_orders_dataset.csv"),
            parse_dates=["order_purchase_timestamp"]
        )
        if last_wm:
            df = df[df["order_purchase_timestamp"] > last_wm]

        if df.empty:
            print(f"  {table}: no new rows since {last_wm}")
            log_run_end(run_id, "success", 0, last_wm)
            return

        df["_ingested_at"] = pd.Timestamp.now()
        df.to_sql(table, engine, if_exists="append", index=False)
        new_wm = df["order_purchase_timestamp"].max()
        log_run_end(run_id, "success", len(df), new_wm)
        print(f"  {table}: {len(df)} rows | watermark={new_wm}")
    except Exception as e:
        log_run_end(run_id, "failed", error=str(e))
        raise


# ------------------------------------------------------------------
# ORDER ITEMS
# ------------------------------------------------------------------
def ingest_order_items():
    table = "bronze_order_items"
    run_id = log_run_start(table)
    try:
        df = pd.read_csv(_csv("olist_order_items_dataset.csv"))
        df["_ingested_at"] = pd.Timestamp.now()
        df.to_sql(table, engine, if_exists="replace", index=False)
        log_run_end(run_id, "success", len(df))
        print(f"  {table}: {len(df)} rows")
    except Exception as e:
        log_run_end(run_id, "failed", error=str(e))
        raise


# ------------------------------------------------------------------
# CUSTOMERS
# ------------------------------------------------------------------
def ingest_customers():
    table = "bronze_customers"
    run_id = log_run_start(table)
    try:
        df = pd.read_csv(_csv("olist_customers_dataset.csv"))
        df["_ingested_at"] = pd.Timestamp.now()
        df.to_sql(table, engine, if_exists="replace", index=False)
        log_run_end(run_id, "success", len(df))
        print(f"  {table}: {len(df)} rows")
    except Exception as e:
        log_run_end(run_id, "failed", error=str(e))
        raise


# ------------------------------------------------------------------
# PRODUCTS
# ------------------------------------------------------------------
def ingest_products():
    table = "bronze_products"
    run_id = log_run_start(table)
    try:
        df = pd.read_csv(_csv("olist_products_dataset.csv"))
        df["_ingested_at"] = pd.Timestamp.now()
        df.to_sql(table, engine, if_exists="replace", index=False)
        log_run_end(run_id, "success", len(df))
        print(f"  {table}: {len(df)} rows")
    except Exception as e:
        log_run_end(run_id, "failed", error=str(e))
        raise


# ------------------------------------------------------------------
# PAYMENTS
# ------------------------------------------------------------------
def ingest_payments():
    table = "bronze_payments"
    run_id = log_run_start(table)
    try:
        df = pd.read_csv(_csv("olist_order_payments_dataset.csv"))
        df["_ingested_at"] = pd.Timestamp.now()
        df.to_sql(table, engine, if_exists="replace", index=False)
        log_run_end(run_id, "success", len(df))
        print(f"  {table}: {len(df)} rows")
    except Exception as e:
        log_run_end(run_id, "failed", error=str(e))
        raise


# ------------------------------------------------------------------
# SELLERS
# ------------------------------------------------------------------
def ingest_sellers():
    table = "bronze_sellers"
    run_id = log_run_start(table)
    try:
        df = pd.read_csv(_csv("olist_sellers_dataset.csv"))
        df["_ingested_at"] = pd.Timestamp.now()
        df.to_sql(table, engine, if_exists="replace", index=False)
        log_run_end(run_id, "success", len(df))
        print(f"  {table}: {len(df)} rows")
    except Exception as e:
        log_run_end(run_id, "failed", error=str(e))
        raise


# ------------------------------------------------------------------
# CATEGORY TRANSLATION
# ------------------------------------------------------------------
def ingest_category_translation():
    table = "bronze_category_translation"
    run_id = log_run_start(table)
    try:
        df = pd.read_csv(_csv("product_category_name_translation.csv"))
        df["_ingested_at"] = pd.Timestamp.now()
        df.to_sql(table, engine, if_exists="replace", index=False)
        log_run_end(run_id, "success", len(df))
        print(f"  {table}: {len(df)} rows")
    except Exception as e:
        log_run_end(run_id, "failed", error=str(e))
        raise


# ------------------------------------------------------------------
# RUN ALL
# ------------------------------------------------------------------
def ingest_all():
    ingest_orders()
    ingest_order_items()
    ingest_customers()
    ingest_products()
    ingest_payments()
    ingest_sellers()
    ingest_category_translation()


if __name__ == "__main__":
    ingest_all()
