"""
utils/watermark.py
------------------
Helpers to read and write watermarks from pipeline_run_log.
Every ingestion/transform function calls these to track run state.
"""
from sqlalchemy import text
from utils.db import get_engine

engine = get_engine()


def get_last_watermark(table_name):
    """Returns last successful watermark timestamp for a table. None = first run."""
    sql = """
        SELECT watermark_value FROM pipeline_run_log
        WHERE table_name = :table AND status = 'success'
        ORDER BY run_finished_at DESC LIMIT 1
    """
    with engine.connect() as conn:
        result = conn.execute(text(sql), {"table": table_name}).fetchone()
    return result[0] if result else None


def log_run_start(table_name):
    """Inserts a 'running' row and returns its run_id."""
    sql = """
        INSERT INTO pipeline_run_log (table_name, run_started_at, status)
        VALUES (:table, NOW(), 'running') RETURNING id
    """
    with engine.connect() as conn:
        result = conn.execute(text(sql), {"table": table_name})
        conn.commit()
        return result.fetchone()[0]


def log_run_end(run_id, status, rows_loaded=0, watermark_value=None, error=None):
    """Updates the run row with final status, row count, watermark, and error."""
    sql = """
        UPDATE pipeline_run_log
        SET run_finished_at = NOW(),
            status          = :status,
            rows_loaded     = :rows,
            watermark_value = :watermark,
            error_message   = :error
        WHERE id = :run_id
    """
    with engine.connect() as conn:
        conn.execute(text(sql), {
            "status": status, "rows": rows_loaded,
            "watermark": watermark_value, "error": error, "run_id": run_id
        })
        conn.commit()
