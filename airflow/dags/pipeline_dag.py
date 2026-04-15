"""
airflow/dags/pipeline_dag.py
-----------------------------
Main Airflow DAG — orchestrates full pipeline end to end.

Flow:
  [ingestion ingest — parallel]
        |
  [Silver clean — parallel]
        |
  [Silver quality checks]
        |
  [Load dims — parallel]
        |
  [Load fact]
        |
  [Gold quality checks]

Schedule: daily at 2 AM
Retries : 2 per task with 5-min delay
SLA     : 3 hours total
"""
import sys
sys.path.insert(0, "/opt/airflow")

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta

from ingestion.ingest import (
    ingest_orders, ingest_order_items, ingest_customers,
    ingest_products, ingest_payments, ingest_sellers,
    ingest_category_translation
)
from transformations.clean import (
    clean_orders, clean_order_items, clean_customers,
    clean_products, clean_payments, clean_sellers
)
from transformations.transform import (
    load_dim_date, load_dim_customers, load_dim_products,
    load_dim_sellers, load_fact_order_items
)
from quality.checks import run_silver_checks, run_gold_checks


default_args = {
    "owner": "data-engineer",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email": ["you@gmail.com"],
    "sla": timedelta(hours=3),
}

with DAG(
    dag_id="ecommerce_pipeline",
    default_args=default_args,
    description="E-commerce Sales Analytics — ingestion > Silver > Gold",
    schedule_interval="0 2 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ecommerce", "batch", "incremental"],
) as dag:

    # ----------------------------------------------------------
    # STEP 1 — ingestion ingestion (all 7 run in parallel)
    # ----------------------------------------------------------
    with TaskGroup("ingest_bronze") as bronze_group:
        t_ing_orders = PythonOperator(
            task_id="orders", python_callable=ingest_orders)
        t_ing_items = PythonOperator(
            task_id="order_items", python_callable=ingest_order_items)
        t_ing_cust = PythonOperator(
            task_id="customers", python_callable=ingest_customers)
        t_ing_prod = PythonOperator(
            task_id="products", python_callable=ingest_products)
        t_ing_pay = PythonOperator(
            task_id="payments", python_callable=ingest_payments)
        t_ing_sell = PythonOperator(
            task_id="sellers", python_callable=ingest_sellers)
        t_ing_cat = PythonOperator(
            task_id="category_translation",
            python_callable=ingest_category_translation)

    # ----------------------------------------------------------
    # STEP 2 — Silver cleaning (all 6 run in parallel)
    # ----------------------------------------------------------
    with TaskGroup("clean_silver") as silver_group:
        t_cln_orders = PythonOperator(
            task_id="orders", python_callable=clean_orders)
        t_cln_items = PythonOperator(
            task_id="order_items", python_callable=clean_order_items)
        t_cln_cust = PythonOperator(
            task_id="customers", python_callable=clean_customers)
        t_cln_prod = PythonOperator(
            task_id="products", python_callable=clean_products)
        t_cln_pay = PythonOperator(
            task_id="payments", python_callable=clean_payments)
        t_cln_sell = PythonOperator(
            task_id="sellers", python_callable=clean_sellers)

    # ----------------------------------------------------------
    # STEP 3 — Silver quality checks
    # ----------------------------------------------------------
    t_silver_checks = PythonOperator(
        task_id="silver_quality_checks",
        python_callable=run_silver_checks
    )

    # ----------------------------------------------------------
    # STEP 4 — Load Gold dimensions (all 4 run in parallel)
    # ----------------------------------------------------------
    with TaskGroup("load_dims") as dims_group:
        t_dim_date = PythonOperator(
            task_id="dim_date", python_callable=load_dim_date)
        t_dim_cust = PythonOperator(
            task_id="dim_customers", python_callable=load_dim_customers)
        t_dim_prod = PythonOperator(
            task_id="dim_products", python_callable=load_dim_products)
        t_dim_sell = PythonOperator(
            task_id="dim_sellers", python_callable=load_dim_sellers)

    # ----------------------------------------------------------
    # STEP 5 — Load fact table
    # ----------------------------------------------------------
    t_fact = PythonOperator(
        task_id="load_fact_order_items",
        python_callable=load_fact_order_items
    )

    # ----------------------------------------------------------
    # STEP 6 — Gold quality checks
    # ----------------------------------------------------------
    t_gold_checks = PythonOperator(
        task_id="gold_quality_checks",
        python_callable=run_gold_checks
    )

    # ----------------------------------------------------------
    # DAG dependency chain
    # ----------------------------------------------------------
    bronze_group >> silver_group >> t_silver_checks >> dims_group >> t_fact >> t_gold_checks
