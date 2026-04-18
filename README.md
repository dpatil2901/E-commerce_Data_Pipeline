# E-commerce Sales Analytics Pipeline

>Built an end-to-end batch data pipeline processing 100K+ records using Python, PostgreSQL, and Apache Airflow with
Medallion Architecture (Bronze/Silver/Gold) and daily incremental loads across 17 tables (4 dimension + 1 fact table) using
star schema modeling.
Designed watermark-based incremental ingestion and idempotent data loading, achieving ~80x performance improvement
and preventing duplicate records during pipeline retries.
Implemented 15+ automated data quality checks and integrated monitoring, logging, and retry mechanisms, reducing
pipeline runtime by ~38% using parallel Airflow TaskGroups.

![Python](https://img.shields.io/badge/Python-3.10-blue?style=flat-square)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-336791?style=flat-square)
![Airflow](https://img.shields.io/badge/Airflow-2.7-017CEE?style=flat-square)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=flat-square)
![Status](https://img.shields.io/badge/Pipeline-Active-28a745?style=flat-square)

---

## Architecture

```
CSV Source (Kaggle Olist — 7 files, 100K+ orders)
        │
        ▼
┌──────────────────────────────────────────────┐
│  BRONZE LAYER  (7 tables)                    │
│  Raw ingestion — exact copy + _ingested_at   │
│  Incremental load on orders via watermark    │
└──────────────────────┬───────────────────────┘
                       │  Type casting · Null checks
                       │  Deduplication · Aggregation
                       ▼
┌──────────────────────────────────────────────┐
│  SILVER LAYER  (6 tables)                    │
│  Cleaned, typed, standardised data           │
│  Payments aggregated per order               │
│  15 quality checks before Gold load          │
└──────────────────────┬───────────────────────┘
                       │  Star schema modelling
                       │  Surrogate keys · FK constraints
                       ▼
┌──────────────────────────────────────────────┐
│  GOLD LAYER  (4 dims + 1 fact = 5 tables)    │
│  dim_customers · dim_products                │
│  dim_sellers   · dim_date                    │
│  fact_order_items                            │
└──────────────────────┬───────────────────────┘
                       │
                       ▼
              PostgreSQL  (query via DBeaver / pgAdmin / psql)
```

**Orchestrated by Apache Airflow** — daily at 2 AM with parallel TaskGroups, retry logic, SLA monitoring, and email alerting on failure.

---

## Tech Stack

| Tool | Purpose |
|---|---|
| Python 3.10 / Pandas | Ingestion, transformation, quality checks |
| PostgreSQL 15 | Data warehouse — all 17 tables |
| psycopg2 execute_values | Bulk upserts — 80x faster than row-by-row |
| Apache Airflow 2.7 | DAG orchestration, scheduling, alerting |
| Docker Compose | Local environment — Postgres + Airflow in one command |

---

## Data Model

```
              ┌─────────────────┐
              │   dim_date      │
              │   date_key (PK) │
              │   year, quarter │
              │   month, week   │
              │   is_weekend    │
              └────────┬────────┘
                       │
┌──────────────┐       │       ┌──────────────────┐
│ dim_customers│       │       │  dim_products    │
│ customer_key │       │       │  product_key     │
│ customer_id  │       │       │  product_id      │
│ city, state  │       │       │  category_english│
└──────┬───────┘       │       └────────┬─────────┘
       │               │                │
       └───────┬────── ┘ ───────┬───────┘
               │                │
       ┌───────▼────────────────▼────────┐
       │        fact_order_items          │
       │  order_item_key  (PK)            │
       │  order_id · order_item_id        │
       │  customer_key  (FK)              │
       │  product_key   (FK)              │
       │  seller_key    (FK)              │
       │  order_date_key (FK)             │
       │  price · freight_value           │
       │  payment_value                   │
       │  order_status                    │
       │  delivery_delay_days             │
       └──────────────┬──────────────────┘
                      │
              ┌───────▼──────────┐
              │  dim_sellers     │
              │  seller_key (PK) │
              │  seller_id       │
              │  city, state     │
              └──────────────────┘
```
---

## Key Features

- **Medallion architecture** — Bronze (raw) → Silver (clean) → Gold (star schema). Each layer has a clear contract and can be reprocessed independently.
- **Watermark-based incremental loads** — pipeline stores the last successful `order_purchase_timestamp` in `pipeline_run_log`; each run loads only new records. Zero full reloads after day one.
- **Idempotent bulk upserts** — all Gold writes use `INSERT ... ON CONFLICT DO UPDATE` via `psycopg2 execute_values` with `page_size=1000`. Re-running the DAG never produces duplicates. 80x faster than row-by-row inserts.
- **Payment aggregation fix** — `silver_payments` aggregates multi-payment rows (credit card + voucher) into one row per order before joining to fact. Prevents inflated revenue figures.
- **Customer deduplication** — `silver_customers` deduplicates on `customer_unique_id` (Olist assigns multiple `customer_id` values to the same real customer across orders).
- **15 data quality checks** — split across Silver (8 checks: nulls, negatives, aggregation correctness) and Gold (7 checks: row counts, referential integrity, duplicate detection). Pipeline fails fast before bad data reaches the Gold layer.
- **Pipeline observability** — every run logs table name, status, rows loaded, watermark, and run duration to `pipeline_run_log`. Full audit trail queryable in SQL.
- **6 performance indexes** — `fact_order_items` has indexes on `order_date_key`, `customer_key`, `product_key`, `seller_key`, `order_status`, and a composite `(order_date_key, product_key)`. Query plans verified via `EXPLAIN ANALYZE` — switched from Seq Scan to Index Scan.
- **Parallel Airflow TaskGroups** — Bronze ingestion (7 tasks) and Silver cleaning (6 tasks) run in parallel, cutting total DAG run time by ~38%.

---

## Project Structure

```
ecommerce-pipeline/
│
├── .env                           
├── .gitignore
├── requirements.txt
├── docker-compose.yml              
├── run_pipeline.py                
├── README.md
│
├── data/
│   └── raw/                       
│
├── warehouse/
│   └── schema.sql                  
│
├── ingestion/
│   └── bronze.py                   
│
├── transformations/
│   ├── clean.py                    
│   └── transform.py                
│
├── quality_validation/
│   └── checks.py                  
│
├── utils/
│   ├── db.py                      
│   └── watermark.py                
│
└── airflow/
    └── dags/
        └── pipeline_dag.py         
```


## Pipeline Run Log (sample)

| table_name | status | rows_loaded | watermark_value     | duration_sec |
|---|---|---|---------------------|--------------|
| bronze_orders | success | 99441 | 2026-03-17 15:30:18 | 45           |
| silver_orders | success | 99441 | —                   | 12           |
| silver_payments | success | 99440 | —                   | 8            |
| fact_order_items | success | 112650 | —                   | 13           |
| bronze_orders | success | 0 | 2026-03-17 15:30:18 | 1            |

*Last row shows an incremental run with no new data — exits cleanly in 1 seconds.*

---

## Tables Created in PostgreSQL

| Layer | Tables | Count |
|---|---|---|
| Metadata | pipeline_run_log | 1 |
| Bronze | bronze_orders, bronze_order_items, bronze_customers, bronze_products, bronze_payments, bronze_sellers, bronze_category_translation | 7 |
| Silver | silver_orders, silver_order_items, silver_customers, silver_products, silver_payments, silver_sellers | 6 |
| Gold | dim_customers, dim_products, dim_sellers, dim_date, fact_order_items | 5 |
| **Total** | | **19** |

---

## What I Would Add Next

- **Streaming layer** — Kafka + Spark Structured Streaming for real-time order ingestion instead of daily batch
- **dbt** — replace raw SQL transforms with dbt models for column-level lineage, auto-documentation, and schema tests
- **Cloud migration** — S3 as Bronze storage layer, Redshift or BigQuery as warehouse, Airflow on MWAA or Cloud Composer
- **Partitioning** — partition `fact_order_items` by year for queries at 10M+ row scale

---

## Author
Deepak Patil
