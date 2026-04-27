"""Silver layer entry point — Bronze → Silver transformations.

Usage:
    python jobs/run_silver.py --bronze-path data/lakehouse/bronze --silver-path data/lakehouse/silver
    python jobs/run_silver.py  # uses defaults
"""

import argparse
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.utils.spark_session import get_spark_session, stop_spark_session
from src.utils.logger import get_logger
from src.utils.io_helpers import read_parquet, write_parquet
from src.transformations.bronze_to_silver import run_bronze_to_silver
from src.quality.data_quality_checks import DataQualityChecker

logger = get_logger(__name__)

# Table definitions: (table_name, primary_key_columns)
SILVER_TABLES = [
    ("orders", ["order_id"]),
    ("order_items", ["order_id", "order_item_id"]),
    ("order_payments", ["order_id", "payment_sequential"]),
    ("order_reviews", ["review_id"]),
    ("products", ["product_id"]),
]


def main():
    parser = argparse.ArgumentParser(description="Silver layer: Bronze → cleansed/conformed")
    parser.add_argument("--bronze-path", default="data/lakehouse/bronze", help="Bronze input path")
    parser.add_argument("--silver-path", default="data/lakehouse/silver", help="Silver output path")
    parser.add_argument("--rules-path", default="configs/data_quality_rules.yaml", help="DQ rules")
    parser.add_argument("--env", default="local", help="Spark environment")
    args = parser.parse_args()

    logger.info(f"Starting Silver transformations: {args.bronze_path} → {args.silver_path}")

    try:
        spark = get_spark_session(env=args.env)
        dq_checker = DataQualityChecker(args.rules_path)

        for table_name, key_cols in SILVER_TABLES:
            logger.info(f"Processing Silver: {table_name}")

            # Read Bronze
            bronze_df = read_parquet(spark, f"{args.bronze_path}/{table_name}")

            # Transform
            silver_df = run_bronze_to_silver(bronze_df, table_name, key_cols)

            # Data quality checks
            dq_checker.run_checks_for_table(silver_df, "bronze_to_silver", table_name)

            if dq_checker.has_critical_failures():
                logger.error(f"Critical DQ failures for {table_name} — halting")
                logger.error(f"DQ Summary: {dq_checker.summary()}")
                sys.exit(1)

            # Write Silver
            partition_cols = None
            if table_name == "orders":
                partition_cols = ["order_purchase_date"]
            write_parquet(silver_df, f"{args.silver_path}/{table_name}",
                          partition_cols=partition_cols, coalesce_num=4)

        logger.info(f"Silver layer complete. DQ Summary: {dq_checker.summary()}")

    except Exception as e:
        logger.error(f"Silver transformation failed: {e}", exc_info=True)
        sys.exit(1)
    finally:
        stop_spark_session()


if __name__ == "__main__":
    main()
