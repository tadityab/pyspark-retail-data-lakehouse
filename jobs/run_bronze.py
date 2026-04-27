"""Bronze layer entry point — ingest raw CSVs into Parquet.

Usage:
    python jobs/run_bronze.py --source-path data/raw/olist --target-path data/lakehouse/bronze
    python jobs/run_bronze.py  # uses defaults from pipeline_config.yaml
"""

import argparse
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.utils.spark_session import get_spark_session, stop_spark_session
from src.utils.logger import get_logger
from src.ingestion.batch_ingestion import ingest_all_olist_tables

logger = get_logger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Bronze layer: raw CSV → Parquet ingestion")
    parser.add_argument("--source-path", default="data/raw/olist", help="Path to raw CSV files")
    parser.add_argument("--target-path", default="data/lakehouse/bronze", help="Bronze output path")
    parser.add_argument("--env", default="local", help="Spark environment (local/docker/production)")
    args = parser.parse_args()

    logger.info(f"Starting Bronze ingestion: {args.source_path} → {args.target_path}")

    try:
        spark = get_spark_session(env=args.env)
        results = ingest_all_olist_tables(spark, args.source_path, args.target_path)

        logger.info("Bronze layer complete:")
        for table, count in results.items():
            logger.info(f"  {table}: {count:,} rows")

    except Exception as e:
        logger.error(f"Bronze ingestion failed: {e}", exc_info=True)
        sys.exit(1)
    finally:
        stop_spark_session()


if __name__ == "__main__":
    main()
