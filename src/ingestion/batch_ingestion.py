"""Batch ingestion — CSV/JSON sources to Bronze layer (Parquet)."""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from src.models.schemas import get_schema
from src.utils.io_helpers import write_parquet
from src.utils.logger import get_logger

logger = get_logger(__name__)


def ingest_csv_to_bronze(
    spark: SparkSession,
    source_path: str,
    target_path: str,
    table_name: str,
    partition_cols: list = None,
) -> DataFrame:
    """Ingest a CSV source file into the Bronze layer as Parquet.

    Bronze layer is schema-on-read: all columns are read as strings
    to preserve raw data exactly as received. Adds metadata columns
    for lineage tracking.

    Args:
        spark: Active SparkSession.
        source_path: Path to source CSV file(s).
        target_path: Bronze output path.
        table_name: Table identifier for schema lookup.
        partition_cols: Optional partition columns.

    Returns:
        DataFrame that was written.
    """
    schema = get_schema("bronze", table_name)

    df = (
        spark.read
        .option("header", "true")
        .option("quote", '"')
        .option("escape", '"')
        .option("multiLine", "true")
        .schema(schema)
        .csv(source_path)
    )

    # Add ingestion metadata
    df_with_metadata = (
        df
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_ingestion_date", F.current_date())
    )

    row_count = df_with_metadata.count()
    logger.info(f"Bronze ingestion [{table_name}]: {row_count} rows from {source_path}")

    write_parquet(
        df_with_metadata,
        target_path,
        partition_cols=partition_cols or ["_ingestion_date"],
        mode="append",
    )

    return df_with_metadata


def ingest_all_olist_tables(spark: SparkSession, source_base: str, bronze_base: str) -> dict:
    """Ingest all Olist CSV tables into Bronze layer.

    Args:
        spark: Active SparkSession.
        source_base: Base path containing Olist CSV files.
        bronze_base: Base path for Bronze Parquet output.

    Returns:
        Dict mapping table name to row count ingested.
    """
    tables = {
        "orders": "olist_orders_dataset.csv",
        "order_items": "olist_order_items_dataset.csv",
        "order_payments": "olist_order_payments_dataset.csv",
        "order_reviews": "olist_order_reviews_dataset.csv",
        "customers": "olist_customers_dataset.csv",
        "sellers": "olist_sellers_dataset.csv",
        "products": "olist_products_dataset.csv",
        "geolocation": "olist_geolocation_dataset.csv",
    }

    results = {}
    for table_name, filename in tables.items():
        source_path = f"{source_base}/{filename}"
        target_path = f"{bronze_base}/{table_name}"

        logger.info(f"Ingesting {table_name} from {source_path}")
        df = ingest_csv_to_bronze(spark, source_path, target_path, table_name)
        results[table_name] = df.count()

    logger.info(f"Bronze ingestion complete: {results}")
    return results
