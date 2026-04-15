"""
utils/db.py
-----------
Centralised DB connection helpers.
Reads credentials from environment variables (set in .env).
"""
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()


def get_engine():
    """Returns SQLAlchemy engine — used for pandas read_sql / to_sql."""
    conn = os.getenv(
        "DB_CONN",
        "postgresql+psycopg2://ecom_user:ecom_pass@localhost:5432/ecommerce"
    )
    return create_engine(conn)


def get_psycopg2_conn_string():
    """Returns raw psycopg2 conn string — used for execute_values bulk upserts."""
    return os.getenv(
        "DB_CONN_PSYCOPG2",
        "host=localhost port=5432 dbname=ecommerce user=ecom_user password=ecom_pass"
    )
