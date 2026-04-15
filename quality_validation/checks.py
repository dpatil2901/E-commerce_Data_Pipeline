"""
quality/checks.py
-----------------
Data quality validation — 15 checks split across Silver and Gold layers.
Raises ValueError on any failure → causes Airflow task to fail fast.

Silver checks  (run after clean_all)  : nulls, types, duplicates, aggregation
Gold checks    (run after transform_all): row counts, referential integrity, dedup
"""
import pandas as pd
from utils.db import get_engine

engine = get_engine()


def run_silver_checks():
    """8 checks on silver layer — run after clean_all()."""
    checks = [
        ("silver_orders: no null order_id",
         "SELECT COUNT(*) FROM silver_orders WHERE order_id IS NULL",
         "eq", 0),

        ("silver_orders: no null customer_id",
         "SELECT COUNT(*) FROM silver_orders WHERE customer_id IS NULL",
         "eq", 0),

        ("silver_order_items: no negative price",
         "SELECT COUNT(*) FROM silver_order_items WHERE price < 0",
         "eq", 0),

        ("silver_order_items: no null order_id",
         "SELECT COUNT(*) FROM silver_order_items WHERE order_id IS NULL",
         "eq", 0),

        ("silver_customers: no null customer_id",
         "SELECT COUNT(*) FROM silver_customers WHERE customer_id IS NULL",
         "eq", 0),

        ("silver_products: no null product_id",
         "SELECT COUNT(*) FROM silver_products WHERE product_id IS NULL",
         "eq", 0),

        ("silver_payments: no negative payment_value",
         "SELECT COUNT(*) FROM silver_payments WHERE payment_value < 0",
         "eq", 0),

        # Critical: payments must be 1 row per order after aggregation
        ("silver_payments: one row per order_id",
         """SELECT COUNT(*) FROM (
              SELECT order_id FROM silver_payments
              GROUP BY order_id HAVING COUNT(*) > 1
            ) t""",
         "eq", 0),
    ]
    _run(checks, "SILVER")


def run_gold_checks():
    """7 checks on gold layer — run after transform_all()."""
    checks = [
        ("fact_order_items: has rows",
         "SELECT COUNT(*) FROM fact_order_items",
         "gt", 0),

        ("dim_customers: has rows",
         "SELECT COUNT(*) FROM dim_customers",
         "gt", 0),

        ("dim_products: has rows",
         "SELECT COUNT(*) FROM dim_products",
         "gt", 0),

        ("dim_date: has rows",
         "SELECT COUNT(*) FROM dim_date",
         "gt", 0),

        # Referential integrity: no orphan customer keys in fact
        ("fact: no orphan customer_key",
         """SELECT COUNT(*) FROM fact_order_items f
            WHERE NOT EXISTS (
              SELECT 1 FROM dim_customers d WHERE d.customer_key = f.customer_key
            )""",
         "eq", 0),

        # Referential integrity: no orphan product keys in fact
        ("fact: no orphan product_key",
         """SELECT COUNT(*) FROM fact_order_items f
            WHERE NOT EXISTS (
              SELECT 1 FROM dim_products d WHERE d.product_key = f.product_key
            )""",
         "eq", 0),

        # Dedup: no duplicate order_id + order_item_id in fact
        ("fact: no duplicate order_item rows",
         """SELECT COUNT(*) FROM (
              SELECT order_id, order_item_id FROM fact_order_items
              GROUP BY order_id, order_item_id HAVING COUNT(*) > 1
            ) t""",
         "eq", 0),
    ]
    _run(checks, "GOLD")


def _run(checks, layer):
    failed = []
    for name, sql, op, threshold in checks:
        result = pd.read_sql(sql, engine).iloc[0, 0]
        passed = (op == "eq" and result == threshold) or \
                 (op == "gt" and result > threshold)
        if passed:
            print(f"  PASS [{layer}] {name}")
        else:
            failed.append(f"  FAIL [{layer}] {name} → got {result}")

    if failed:
        raise ValueError("\n".join(failed))
    print(f"\n  All {layer} quality checks passed.\n")


if __name__ == "__main__":
    run_silver_checks()
    run_gold_checks()
