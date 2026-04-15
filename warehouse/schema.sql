-- ============================================================
-- PIPELINE RUN LOG
-- ============================================================
CREATE TABLE IF NOT EXISTS pipeline_run_log (
    id              SERIAL PRIMARY KEY,
    table_name      TEXT NOT NULL,
    run_started_at  TIMESTAMP NOT NULL,
    run_finished_at TIMESTAMP,
    status          TEXT CHECK (status IN ('running','success','failed')),
    rows_loaded     INT,
    watermark_value TIMESTAMP,
    error_message   TEXT
);
CREATE INDEX IF NOT EXISTS idx_run_log_table ON pipeline_run_log(table_name, status);

-- ============================================================
-- BRONZE LAYER
-- ============================================================
CREATE TABLE IF NOT EXISTS bronze_orders (
    order_id TEXT, customer_id TEXT, order_status TEXT,
    order_purchase_timestamp TEXT, order_approved_at TEXT,
    order_delivered_carrier_date TEXT, order_delivered_customer_date TEXT,
    order_estimated_delivery_date TEXT, _ingested_at TIMESTAMP
);
CREATE TABLE IF NOT EXISTS bronze_order_items (
    order_id TEXT, order_item_id INT, product_id TEXT, seller_id TEXT,
    shipping_limit_date TEXT, price TEXT, freight_value TEXT, _ingested_at TIMESTAMP
);
CREATE TABLE IF NOT EXISTS bronze_customers (
    customer_id TEXT, customer_unique_id TEXT, customer_zip_code_prefix TEXT,
    customer_city TEXT, customer_state TEXT, _ingested_at TIMESTAMP
);
CREATE TABLE IF NOT EXISTS bronze_products (
    product_id TEXT, product_category_name TEXT, product_weight_g TEXT,
    product_length_cm TEXT, product_height_cm TEXT, product_width_cm TEXT,
    _ingested_at TIMESTAMP
);
CREATE TABLE IF NOT EXISTS bronze_payments (
    order_id TEXT, payment_sequential INT, payment_type TEXT,
    payment_installments INT, payment_value TEXT, _ingested_at TIMESTAMP
);
CREATE TABLE IF NOT EXISTS bronze_sellers (
    seller_id TEXT, seller_zip_code_prefix TEXT,
    seller_city TEXT, seller_state TEXT, _ingested_at TIMESTAMP
);
CREATE TABLE IF NOT EXISTS bronze_category_translation (
    product_category_name TEXT, product_category_name_english TEXT,
    _ingested_at TIMESTAMP
);

-- ============================================================
-- SILVER LAYER
-- ============================================================
CREATE TABLE IF NOT EXISTS silver_orders (
    order_id TEXT, customer_id TEXT, order_status TEXT,
    order_purchase_timestamp TIMESTAMP, order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP, order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP, delivery_delay_days INT, _cleaned_at TIMESTAMP
);
CREATE TABLE IF NOT EXISTS silver_order_items (
    order_id TEXT, order_item_id INT, product_id TEXT, seller_id TEXT,
    shipping_limit_date TIMESTAMP, price NUMERIC(10,2),
    freight_value NUMERIC(10,2), _cleaned_at TIMESTAMP
);
CREATE TABLE IF NOT EXISTS silver_customers (
    customer_id TEXT, customer_unique_id TEXT, zip_code_prefix TEXT,
    city TEXT, state TEXT, _cleaned_at TIMESTAMP
);
CREATE TABLE IF NOT EXISTS silver_products (
    product_id TEXT, category_english TEXT, weight_g NUMERIC(10,2),
    length_cm NUMERIC(10,2), height_cm NUMERIC(10,2),
    width_cm NUMERIC(10,2), _cleaned_at TIMESTAMP
);
CREATE TABLE IF NOT EXISTS silver_payments (
    order_id TEXT, payment_type TEXT,
    payment_value NUMERIC(10,2), _cleaned_at TIMESTAMP
);
CREATE TABLE IF NOT EXISTS silver_sellers (
    seller_id TEXT, zip_code_prefix TEXT,
    city TEXT, state TEXT, _cleaned_at TIMESTAMP
);

-- ============================================================
-- GOLD LAYER
-- ============================================================
CREATE TABLE IF NOT EXISTS dim_customers (
    customer_key SERIAL PRIMARY KEY, customer_id TEXT UNIQUE NOT NULL,
    customer_unique_id TEXT, city TEXT, state TEXT, zip_prefix TEXT
);
CREATE TABLE IF NOT EXISTS dim_products (
    product_key SERIAL PRIMARY KEY, product_id TEXT UNIQUE NOT NULL,
    category_english TEXT, weight_g NUMERIC(10,2),
    length_cm NUMERIC(10,2), height_cm NUMERIC(10,2), width_cm NUMERIC(10,2)
);
CREATE TABLE IF NOT EXISTS dim_sellers (
    seller_key SERIAL PRIMARY KEY, seller_id TEXT UNIQUE NOT NULL,
    city TEXT, state TEXT, zip_prefix TEXT
);
CREATE TABLE IF NOT EXISTS dim_date (
    date_key INT PRIMARY KEY, full_date DATE, year INT, quarter INT,
    month INT, month_name TEXT, week INT, day_of_week TEXT, is_weekend BOOLEAN
);
CREATE TABLE IF NOT EXISTS fact_order_items (
    order_item_key SERIAL PRIMARY KEY,
    order_id TEXT, order_item_id INT,
    customer_key INT REFERENCES dim_customers(customer_key),
    product_key  INT REFERENCES dim_products(product_key),
    seller_key   INT REFERENCES dim_sellers(seller_key),
    order_date_key INT REFERENCES dim_date(date_key),
    price NUMERIC(10,2), freight_value NUMERIC(10,2), payment_value NUMERIC(10,2),
    order_status TEXT, delivery_delay_days INT,
    CONSTRAINT uq_order_item UNIQUE (order_id, order_item_id)
);

-- ============================================================
-- INDEXES
-- ============================================================
CREATE INDEX IF NOT EXISTS idx_fact_order_date   ON fact_order_items (order_date_key);
CREATE INDEX IF NOT EXISTS idx_fact_customer     ON fact_order_items (customer_key);
CREATE INDEX IF NOT EXISTS idx_fact_product      ON fact_order_items (product_key);
CREATE INDEX IF NOT EXISTS idx_fact_seller       ON fact_order_items (seller_key);
CREATE INDEX IF NOT EXISTS idx_fact_status       ON fact_order_items (order_status);
CREATE INDEX IF NOT EXISTS idx_fact_date_product ON fact_order_items (order_date_key, product_key);
