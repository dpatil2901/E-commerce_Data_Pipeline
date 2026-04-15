"""
run_pipeline.py
---------------
Single entry point to run the full pipeline end to end.
Order: ingestion ingestion -> Silver cleaning -> Silver checks
       -> Gold transformation -> Gold checks

Usage:
    python run_pipeline.py
"""
import time
from ingestion.ingest import ingest_all
from transformations.clean import clean_all
from transformations.transform import transform_all
from quality.checks import run_silver_checks, run_gold_checks


def run():
    total_start = time.time()

    print("=" * 55)
    print("  E-COMMERCE SALES ANALYTICS PIPELINE")
    print("=" * 55)

    print("\n[STEP 1/5] ingestion ingestion...")
    t = time.time()
    ingest_all()
    print(f"  Done in {round(time.time() - t, 1)}s")

    print("\n[STEP 2/5] Silver cleaning...")
    t = time.time()
    clean_all()
    print(f"  Done in {round(time.time() - t, 1)}s")

    print("\n[STEP 3/5] Silver quality checks...")
    t = time.time()
    run_silver_checks()
    print(f"  Done in {round(time.time() - t, 1)}s")

    print("\n[STEP 4/5] Gold transformation...")
    t = time.time()
    transform_all()
    print(f"  Done in {round(time.time() - t, 1)}s")

    print("\n[STEP 5/5] Gold quality checks...")
    t = time.time()
    run_gold_checks()
    print(f"  Done in {round(time.time() - t, 1)}s")

    print("\n" + "=" * 55)
    print(f"  PIPELINE COMPLETE in {round(time.time() - total_start, 1)}s")
    print("=" * 55)


if __name__ == "__main__":
    run()
