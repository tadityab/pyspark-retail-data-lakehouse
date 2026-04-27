"""Quarantine handler — routes failed records to rejected path with failure reason."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.utils.io_helpers import write_parquet
from src.utils.logger import get_logger

logger = get_logger(__name__)


def quarantine_records(
    df: DataFrame,
    condition,
    rejected_path: str,
    table_name: str,
    failure_reason: str,
) -> tuple[DataFrame, DataFrame]:
    """Split a DataFrame into valid and rejected records.

    Failed records are written to a rejected path with metadata
    explaining why they were quarantined.

    Args:
        df: Input DataFrame.
        condition: PySpark Column expression — rows matching this are quarantined.
        rejected_path: Base path for rejected records.
        table_name: Table name for organizing rejected output.
        failure_reason: Human-readable reason for quarantine.

    Returns:
        Tuple of (valid_df, rejected_df).
    """
    rejected = (
        df.filter(condition)
        .withColumn("_rejection_reason", F.lit(failure_reason))
        .withColumn("_rejected_at", F.current_timestamp())
        .withColumn("_source_table", F.lit(table_name))
    )

    valid = df.filter(~condition)

    rejected_count = rejected.count()
    if rejected_count > 0:
        output_path = f"{rejected_path}/{table_name}"
        write_parquet(
            rejected,
            output_path,
            partition_cols=["_source_table"],
            mode="append",
        )
        logger.warning(
            f"Quarantined {rejected_count} records from {table_name}: {failure_reason}"
        )
    else:
        logger.info(f"No records quarantined for {table_name}: {failure_reason}")

    return valid, rejected


def quarantine_nulls(
    df: DataFrame,
    required_columns: list,
    rejected_path: str,
    table_name: str,
) -> DataFrame:
    """Remove and quarantine rows with nulls in required columns.

    Args:
        df: Input DataFrame.
        required_columns: Columns that must not be null.
        rejected_path: Path for rejected output.
        table_name: Table name for context.

    Returns:
        DataFrame with null rows removed.
    """
    null_condition = F.lit(False)
    for col in required_columns:
        null_condition = null_condition | F.col(col).isNull()

    valid, _ = quarantine_records(
        df=df,
        condition=null_condition,
        rejected_path=rejected_path,
        table_name=table_name,
        failure_reason=f"Null values in required columns: {required_columns}",
    )

    return valid
