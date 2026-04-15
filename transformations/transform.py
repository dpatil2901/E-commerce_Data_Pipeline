"""
transformations/transform.py
-----------------------------
Gold layer — loads all 4 dimension tables + fact table into PostgreSQL.

Uses psycopg2 execute_values for bulk idempotent upserts:
  - INSERT ... ON CONFLICT DO UPDATE  (never duplicates on re-run)
  - page_size=1000                    (50-100x faster than row-by-row)

dim_date is generated in Python — no source CSV needed.
"""
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from utils.db import get_engine, get_psycopg2_conn_string
from utils.watermark import log_run_start, log_run_end

engine = get_engine()


def _bulk_upsert(sql, records, page_size=1000):
    """Generic bulk upsert using psycopg2 execute_values."""
    conn_string = get_psycopg2_conn_string()
    with psycopg2.connect(conn_string) as conn:
        with conn.cursor() as cur:
            execute_values(cur, sql, records, page_size=page_size)
        conn.commit()


# ------------------------------------------------------------------
# DIM DATE  (generated 2000–2030, no CSV needed)
# ------------------------------------------------------------------
def load_dim_date():
    table = "dim_date"
    run_id = log_run_start(table)
    try:
        dates = pd.date_range("2000-01-01", "2030-12-31", freq="D")
        df = pd.DataFrame({
            "date_key":    dates.strftime("%Y%m%d").astype(int),
            "full_date":   dates.date,
            "year":        dates.year,
            "quarter":     dates.quarter,
            "month":       dates.month,
            "month_name":  dates.strftime("%B"),
            "week":        dates.isocalendar().week.astype(int),
            "day_of_week": dates.strftime("%A"),
            "is_weekend":  dates.weekday >= 5
        })

        sql = """
            INSERT INTO dim_date
                (date_key, full_date, year, quarter, month,
                 month_name, week, day_of_week, is_weekend)
            VALUES %s
            ON CONFLICT (date_key) DO NOTHING
        """
        records = list(df.itertuples(index=False, name=None))
        _bulk_upsert(sql, records)

        log_run_end(run_id, "success", len(df))
        print(f"  {table}: {len(df)} rows")
    except Exception as e:
        log_run_end(run_id, "failed", error=str(e))
        raise


# ------------------------------------------------------------------
# DIM CUSTOMERS
# ------------------------------------------------------------------
def load_dim_customers():
    table = "dim_customers"
    run_id = log_run_start(table)
    try:
        df = pd.read_sql("""
            SELECT customer_id, customer_unique_id,
                   city, state, zip_code_prefix AS zip_prefix
            FROM silver_customers
        """, engine)

        sql = """
            INSERT INTO dim_customers
                (customer_id, customer_unique_id, city, state, zip_prefix)
            VALUES %s
            ON CONFLICT (customer_id) DO UPDATE SET
                city       = EXCLUDED.city,
                state      = EXCLUDED.state,
                zip_prefix = EXCLUDED.zip_prefix
        """
        records = list(df.itertuples(index=False, name=None))
        _bulk_upsert(sql, records)

        log_run_end(run_id, "success", len(df))
        print(f"  {table}: {len(df)} rows")
    except Exception as e:
        log_run_end(run_id, "failed", error=str(e))
        raise


# ------------------------------------------------------------------
# DIM PRODUCTS
# ------------------------------------------------------------------
def load_dim_products():
    table = "dim_products"
    run_id = log_run_start(table)
    try:
        df = pd.read_sql("""
            SELECT product_id, category_english,
                   weight_g, length_cm, height_cm, width_cm
            FROM silver_products
        """, engine)

        sql = """
            INSERT INTO dim_products
                (product_id, category_english,
                 weight_g, length_cm, height_cm, width_cm)
            VALUES %s
            ON CONFLICT (product_id) DO UPDATE SET
                category_english = EXCLUDED.category_english,
                weight_g         = EXCLUDED.weight_g,
                length_cm        = EXCLUDED.length_cm,
                height_cm        = EXCLUDED.height_cm,
                width_cm         = EXCLUDED.width_cm
        """
        records = list(df.itertuples(index=False, name=None))
        _bulk_upsert(sql, records)

        log_run_end(run_id, "success", len(df))
        print(f"  {table}: {len(df)} rows")
    except Exception as e:
        log_run_end(run_id, "failed", error=str(e))
        raise


# ------------------------------------------------------------------
# DIM SELLERS
# ------------------------------------------------------------------
def load_dim_sellers():
    table = "dim_sellers"
    run_id = log_run_start(table)
    try:
        df = pd.read_sql("""
            SELECT seller_id, city, state,
                   zip_code_prefix AS zip_prefix
            FROM silver_sellers
        """, engine)

        sql = """
            INSERT INTO dim_sellers (seller_id, city, state, zip_prefix)
            VALUES %s
            ON CONFLICT (seller_id) DO UPDATE SET
                city       = EXCLUDED.city,
                state      = EXCLUDED.state,
                zip_prefix = EXCLUDED.zip_prefix
        """
        records = list(df.itertuples(index=False, name=None))
        _bulk_upsert(sql, records)

        log_run_end(run_id, "success", len(df))
        print(f"  {table}: {len(df)} rows")
    except Exception as e:
        log_run_end(run_id, "failed", error=str(e))
        raise


# ------------------------------------------------------------------
# FACT ORDER ITEMS  (joins all silver + dim lookup keys)
# ------------------------------------------------------------------
def load_fact_order_items():
    table = "fact_order_items"
    run_id = log_run_start(table)
    try:
        df = pd.read_sql("""
            SELECT
                oi.order_id,
                oi.order_item_id,
                dc.customer_key,
                dp.product_key,
                ds.seller_key,
                CAST(TO_CHAR(o.order_purchase_timestamp, 'YYYYMMDD') AS INT) AS order_date_key,
                oi.price,
                oi.freight_value,
                COALESCE(p.payment_value, 0) AS payment_value,
                o.order_status,
                o.delivery_delay_days
            FROM silver_order_items    oi
            JOIN silver_orders         o   ON oi.order_id  = o.order_id
            JOIN dim_customers         dc  ON o.customer_id = dc.customer_id
            JOIN dim_products          dp  ON oi.product_id = dp.product_id
            JOIN dim_sellers           ds  ON oi.seller_id  = ds.seller_id
            LEFT JOIN silver_payments  p   ON oi.order_id  = p.order_id
        """, engine)

        # Drop rows where any FK is missing
        df = df.dropna(subset=["customer_key", "product_key", "seller_key", "order_date_key"])
        df[["customer_key", "product_key", "seller_key", "order_date_key"]] = \
            df[["customer_key", "product_key", "seller_key", "order_date_key"]].astype(int)

        sql = """
            INSERT INTO fact_order_items (
                order_id, order_item_id, customer_key, product_key, seller_key,
                order_date_key, price, freight_value, payment_value,
                order_status, delivery_delay_days
            )
            VALUES %s
            ON CONFLICT (order_id, order_item_id) DO UPDATE SET
                order_status        = EXCLUDED.order_status,
                payment_value       = EXCLUDED.payment_value,
                delivery_delay_days = EXCLUDED.delivery_delay_days
        """
        records = list(df.itertuples(index=False, name=None))
        _bulk_upsert(sql, records, page_size=1000)

        log_run_end(run_id, "success", len(df))
        print(f"  {table}: {len(df)} rows")
    except Exception as e:
        log_run_end(run_id, "failed", error=str(e))
        raise


# ------------------------------------------------------------------
# RUN ALL
# ------------------------------------------------------------------
def transform_all():
    load_dim_date()
    load_dim_customers()
    load_dim_products()
    load_dim_sellers()
    load_fact_order_items()


if __name__ == "__main__":
    transform_all()
