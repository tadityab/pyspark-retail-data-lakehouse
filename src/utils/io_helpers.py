"""I/O helpers for reading/writing data with partition management."""

import os
from pyspark.sql import DataFrame, SparkSession
from src.utils.logger import get_logger

logger = get_logger(__name__)


def read_csv(spark: SparkSession, path: str, schema=None, header: bool = True,
             infer_schema: bool = True) -> DataFrame:
    """Read CSV files with optional schema enforcement."""
    reader = spark.read.option("header", str(header).lower())
    if schema:
        reader = reader.schema(schema)
    else:
        reader = reader.option("inferSchema", str(infer_schema).lower())
    df = reader.csv(path)
    logger.info(f"Read {df.count()} rows from {path}")
    return df


def write_parquet(df: DataFrame, path: str, partition_cols: list = None,
                  mode: str = "overwrite", coalesce_num: int = None):
    """Write DataFrame as Parquet with optional partitioning."""
    writer = df
    if coalesce_num:
        writer = writer.coalesce(coalesce_num)
    writer = writer.write.mode(mode)
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.parquet(path)
    logger.info(f"Wrote Parquet to {path} (mode={mode}, partitions={partition_cols})")


def read_parquet(spark: SparkSession, path: str) -> DataFrame:
    """Read Parquet files from a path."""
    df = spark.read.parquet(path)
    logger.info(f"Read Parquet from {path}, columns: {df.columns}")
    return df


def write_delta(df: DataFrame, path: str, partition_cols: list = None,
                mode: str = "overwrite", coalesce_num: int = None):
    """Write DataFrame in Delta format; fallback to Parquet if Delta unavailable."""
    writer = df
    if coalesce_num:
        writer = writer.coalesce(coalesce_num)

    try:
        delta_writer = writer.write.format("delta").mode(mode)
        if partition_cols:
            delta_writer = delta_writer.partitionBy(*partition_cols)
        delta_writer.save(path)
        logger.info(f"Wrote Delta to {path} (mode={mode}, partitions={partition_cols})")
    except Exception as exc:
        logger.warning(
            "Delta write failed for %s; falling back to Parquet. Error: %s",
            path,
            exc,
        )
        parquet_writer = writer.write.mode(mode)
        if partition_cols:
            parquet_writer = parquet_writer.partitionBy(*partition_cols)
        parquet_writer.parquet(path)
        logger.info(
            f"Wrote Parquet fallback to {path} (mode={mode}, partitions={partition_cols})"
        )


def read_delta(spark: SparkSession, path: str) -> DataFrame:
    """Read Delta table from a path; fallback to Parquet if needed."""
    try:
        df = spark.read.format("delta").load(path)
        logger.info(f"Read Delta from {path}, columns: {df.columns}")
        return df
    except Exception as exc:
        logger.warning(
            "Delta read failed for %s; falling back to Parquet. Error: %s",
            path,
            exc,
        )
        df = spark.read.parquet(path)
        logger.info(f"Read Parquet fallback from {path}, columns: {df.columns}")
        return df


def path_exists(path: str) -> bool:
    """Check if a local filesystem path exists."""
    return os.path.exists(path)
