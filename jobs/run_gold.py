"""Gold layer entry point — Silver → business-ready analytics tables.

Usage:
    python jobs/run_gold.py --silver-path data/lakehouse/silver --gold-path data/lakehouse/gold
    python jobs/run_gold.py  # uses defaults
"""

import argparse
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.utils.spark_session import get_spark_session, stop_spark_session
from src.utils.logger import get_logger
from src.transformations.silver_to_gold import build_all_gold_tables

logger = get_logger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Gold layer: Silver → analytics-ready tables")
    parser.add_argument("--silver-path", default="data/lakehouse/silver", help="Silver input path")
    parser.add_argument("--gold-path", default="data/lakehouse/gold", help="Gold output path")
    parser.add_argument("--env", default="local", help="Spark environment")
    args = parser.parse_args()

    logger.info(f"Starting Gold aggregations: {args.silver_path} → {args.gold_path}")

    try:
        spark = get_spark_session(env=args.env)
        results = build_all_gold_tables(spark, args.silver_path, args.gold_path)

        logger.info("Gold layer complete:")
        for table, count in results.items():
            logger.info(f"  {table}: {count:,} rows")

    except Exception as e:
        logger.error(f"Gold aggregation failed: {e}", exc_info=True)
        sys.exit(1)
    finally:
        stop_spark_session()


if __name__ == "__main__":
    main()
