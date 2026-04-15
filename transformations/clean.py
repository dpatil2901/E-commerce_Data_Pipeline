"""
transformations/clean.py
------------------------
Silver layer — cleans all 6 ingestion tables and writes to silver tables.

Key cleaning per table:
  orders       : cast timestamps, drop null PKs, derive delivery_delay_days
  order_items  : cast price/freight to NUMERIC, drop negatives
  customers    : deduplicate on customer_unique_id (Olist quirk)
  products     : join category translation, fill nulls with 'unknown'
  payments     : AGGREGATE per order_id — critical to prevent inflated revenue
  sellers      : strip/standardise city and state
"""
import pandas as pd
from utils.db import get_engine
from utils.watermark import log_run_start, log_run_end

engine = get_engine()


# ------------------------------------------------------------------
# ORDERS
# ------------------------------------------------------------------
def clean_orders():
    table = "silver_orders"
    run_id = log_run_start(table)
    try:
        df = pd.read_sql("SELECT * FROM bronze_orders", engine)

        ts_cols = [
            "order_purchase_timestamp", "order_approved_at",
            "order_delivered_carrier_date", "order_delivered_customer_date",
            "order_estimated_delivery_date"
        ]
        for col in ts_cols:
            df[col] = pd.to_datetime(df[col], errors="coerce")

        # Drop rows missing primary identifiers
        df = df.dropna(subset=["order_id", "customer_id"])
        df["order_status"] = df["order_status"].str.strip().str.lower()

        # Derived column: positive = late, negative = early
        df["delivery_delay_days"] = (
            df["order_delivered_customer_date"] -
            df["order_estimated_delivery_date"]
        ).dt.days

        df = df.drop(columns=["_ingested_at"], errors="ignore")
        df["_cleaned_at"] = pd.Timestamp.now()

        df.to_sql(table, engine, if_exists="replace", index=False)
        log_run_end(run_id, "success", len(df))
        print(f"  {table}: {len(df)} rows")
    except Exception as e:
        log_run_end(run_id, "failed", error=str(e))
        raise


# ------------------------------------------------------------------
# ORDER ITEMS
# ------------------------------------------------------------------
def clean_order_items():
    table = "silver_order_items"
    run_id = log_run_start(table)
    try:
        df = pd.read_sql("SELECT * FROM bronze_order_items", engine)

        df = df.dropna(subset=["order_id", "product_id"])
        df["price"] = pd.to_numeric(df["price"], errors="coerce").fillna(0)
        df["freight_value"] = pd.to_numeric(df["freight_value"], errors="coerce").fillna(0)

        # Remove rows with negative prices
        df = df[df["price"] >= 0]
        df["shipping_limit_date"] = pd.to_datetime(df["shipping_limit_date"], errors="coerce")

        df = df.drop(columns=["_ingested_at"], errors="ignore")
        df["_cleaned_at"] = pd.Timestamp.now()

        df.to_sql(table, engine, if_exists="replace", index=False)
        log_run_end(run_id, "success", len(df))
        print(f"  {table}: {len(df)} rows")
    except Exception as e:
        log_run_end(run_id, "failed", error=str(e))
        raise


# ------------------------------------------------------------------
# CUSTOMERS  (deduplicate on customer_unique_id)
# ------------------------------------------------------------------
def clean_customers():
    table = "silver_customers"
    run_id = log_run_start(table)
    try:
        df = pd.read_sql("SELECT * FROM bronze_customers", engine)

        df = df.dropna(subset=["customer_id", "customer_unique_id"])
        df["customer_city"] = df["customer_city"].str.strip().str.lower()
        df["customer_state"] = df["customer_state"].str.strip().str.upper()

        # Olist quirk: same real customer has multiple customer_id values
        # Keep one row per unique real customer (last record wins)
        df = df.sort_values("customer_id").drop_duplicates(
            subset=["customer_unique_id"], keep="last"
        )

        df = df.rename(columns={
            "customer_zip_code_prefix": "zip_code_prefix",
            "customer_city": "city",
            "customer_state": "state"
        })
        df = df[["customer_id", "customer_unique_id", "zip_code_prefix", "city", "state"]]
        df["_cleaned_at"] = pd.Timestamp.now()

        df.to_sql(table, engine, if_exists="replace", index=False)
        log_run_end(run_id, "success", len(df))
        print(f"  {table}: {len(df)} rows")
    except Exception as e:
        log_run_end(run_id, "failed", error=str(e))
        raise


# ------------------------------------------------------------------
# PRODUCTS  (join category translation, cast numerics)
# ------------------------------------------------------------------
def clean_products():
    table = "silver_products"
    run_id = log_run_start(table)
    try:
        df = pd.read_sql("SELECT * FROM bronze_products", engine)
        cat = pd.read_sql(
            "SELECT product_category_name, product_category_name_english "
            "FROM bronze_category_translation",
            engine
        )

        df = df.dropna(subset=["product_id"])
        df = df.merge(cat, on="product_category_name", how="left")
        df["category_english"] = df["product_category_name_english"].fillna("unknown")

        num_cols = {
            "product_weight_g": "weight_g",
            "product_length_cm": "length_cm",
            "product_height_cm": "height_cm",
            "product_width_cm": "width_cm"
        }
        for src, dst in num_cols.items():
            df[dst] = pd.to_numeric(df[src], errors="coerce").fillna(0)

        df = df[["product_id", "category_english", "weight_g", "length_cm", "height_cm", "width_cm"]]
        df["_cleaned_at"] = pd.Timestamp.now()

        df.to_sql(table, engine, if_exists="replace", index=False)
        log_run_end(run_id, "success", len(df))
        print(f"  {table}: {len(df)} rows")
    except Exception as e:
        log_run_end(run_id, "failed", error=str(e))
        raise


# ------------------------------------------------------------------
# PAYMENTS  (aggregate per order — prevents duplicate fact rows)
# ------------------------------------------------------------------
def clean_payments():
    table = "silver_payments"
    run_id = log_run_start(table)
    try:
        df = pd.read_sql("SELECT * FROM bronze_payments", engine)

        df = df.dropna(subset=["order_id"])
        df["payment_value"] = pd.to_numeric(df["payment_value"], errors="coerce").fillna(0)

        # One order can have multiple rows (e.g. credit card + voucher combined)
        # Step 1: find dominant payment type per order
        dominant_type = (
            df.groupby(["order_id", "payment_type"])
            .size()
            .reset_index(name="count")
            .sort_values("count", ascending=False)
            .drop_duplicates("order_id")[["order_id", "payment_type"]]
        )
        # Step 2: sum total payment_value per order
        totals = df.groupby("order_id")["payment_value"].sum().reset_index()

        # Step 3: merge
        result = totals.merge(dominant_type, on="order_id")
        result["_cleaned_at"] = pd.Timestamp.now()

        result.to_sql(table, engine, if_exists="replace", index=False)
        log_run_end(run_id, "success", len(result))
        print(f"  {table}: {len(result)} rows (aggregated from {len(df)} raw rows)")
    except Exception as e:
        log_run_end(run_id, "failed", error=str(e))
        raise


# ------------------------------------------------------------------
# SELLERS
# ------------------------------------------------------------------
def clean_sellers():
    table = "silver_sellers"
    run_id = log_run_start(table)
    try:
        df = pd.read_sql("SELECT * FROM bronze_sellers", engine)

        df = df.dropna(subset=["seller_id"])
        df["seller_city"] = df["seller_city"].str.strip().str.lower()
        df["seller_state"] = df["seller_state"].str.strip().str.upper()
        df = df.drop_duplicates(subset=["seller_id"])

        df = df.rename(columns={
            "seller_zip_code_prefix": "zip_code_prefix",
            "seller_city": "city",
            "seller_state": "state"
        })
        df = df[["seller_id", "zip_code_prefix", "city", "state"]]
        df["_cleaned_at"] = pd.Timestamp.now()

        df.to_sql(table, engine, if_exists="replace", index=False)
        log_run_end(run_id, "success", len(df))
        print(f"  {table}: {len(df)} rows")
    except Exception as e:
        log_run_end(run_id, "failed", error=str(e))
        raise


# ------------------------------------------------------------------
# RUN ALL
# ------------------------------------------------------------------
def clean_all():
    clean_orders()
    clean_order_items()
    clean_customers()
    clean_products()
    clean_payments()
    clean_sellers()


if __name__ == "__main__":
    clean_all()
