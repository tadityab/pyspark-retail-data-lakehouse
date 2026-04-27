"""Full pipeline entry point — Bronze → Silver → Gold with DQ gates.

Usage:
    python jobs/run_full_pipeline.py
    python jobs/run_full_pipeline.py --env docker --source-path /data/raw/olist
"""

import argparse
import sys
import os
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.utils.spark_session import get_spark_session, stop_spark_session
from src.utils.logger import get_logger
from src.utils.io_helpers import read_parquet, write_parquet
from src.ingestion.batch_ingestion import ingest_all_olist_tables
from src.transformations.bronze_to_silver import run_bronze_to_silver
from src.transformations.silver_to_gold import build_all_gold_tables
from src.quality.data_quality_checks import DataQualityChecker

logger = get_logger(__name__)

SILVER_TABLES = [
    ("orders", ["order_id"]),
    ("order_items", ["order_id", "order_item_id"]),
    ("order_payments", ["order_id", "payment_sequential"]),
    ("order_reviews", ["review_id"]),
    ("products", ["product_id"]),
]


def main():
    parser = argparse.ArgumentParser(description="Full pipeline: Bronze → Silver → Gold")
    parser.add_argument("--source-path", default="data/raw/olist", help="Raw CSV source path")
    parser.add_argument("--lakehouse-path", default="data/lakehouse", help="Lakehouse base path")
    parser.add_argument("--rules-path", default="configs/data_quality_rules.yaml", help="DQ rules")
    parser.add_argument("--env", default="local", help="Spark environment")
    args = parser.parse_args()

    bronze_path = f"{args.lakehouse_path}/bronze"
    silver_path = f"{args.lakehouse_path}/silver"
    gold_path = f"{args.lakehouse_path}/gold"

    start_time = datetime.now(timezone.utc)
    logger.info(f"Pipeline started at {start_time.isoformat()}")

    try:
        spark = get_spark_session(env=args.env)

        # ── BRONZE ──────────────────────────────────────────────
        logger.info("=" * 60)
        logger.info("STAGE 1: Bronze Ingestion")
        logger.info("=" * 60)
        bronze_results = ingest_all_olist_tables(spark, args.source_path, bronze_path)

        # ── SILVER ──────────────────────────────────────────────
        logger.info("=" * 60)
        logger.info("STAGE 2: Silver Transformations")
        logger.info("=" * 60)
        dq_checker = DataQualityChecker(args.rules_path)

        for table_name, key_cols in SILVER_TABLES:
            logger.info(f"Silver: {table_name}")
            bronze_df = read_parquet(spark, f"{bronze_path}/{table_name}")
            silver_df = run_bronze_to_silver(bronze_df, table_name, key_cols)

            dq_checker.run_checks_for_table(silver_df, "bronze_to_silver", table_name)
            if dq_checker.has_critical_failures():
                logger.error(f"PIPELINE HALTED: Critical DQ failure in {table_name}")
                logger.error(f"DQ Summary: {dq_checker.summary()}")
                sys.exit(1)

            partition_cols = ["order_purchase_date"] if table_name == "orders" else None
            write_parquet(silver_df, f"{silver_path}/{table_name}",
                          partition_cols=partition_cols, coalesce_num=4)

        logger.info(f"Silver DQ Summary: {dq_checker.summary()}")

        # ── GOLD ────────────────────────────────────────────────
        logger.info("=" * 60)
        logger.info("STAGE 3: Gold Aggregations")
        logger.info("=" * 60)
        gold_results = build_all_gold_tables(spark, silver_path, gold_path)

        # ── SUMMARY ─────────────────────────────────────────────
        end_time = datetime.now(timezone.utc)
        duration = (end_time - start_time).total_seconds()

        logger.info("=" * 60)
        logger.info("PIPELINE COMPLETE")
        logger.info(f"Duration: {duration:.1f}s")
        logger.info(f"Bronze tables: {len(bronze_results)}")
        logger.info(f"Silver tables: {len(SILVER_TABLES)}")
        logger.info(f"Gold tables: {len(gold_results)}")
        logger.info(f"DQ checks: {dq_checker.summary()}")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        sys.exit(1)
    finally:
        stop_spark_session()


if __name__ == "__main__":
    main()
