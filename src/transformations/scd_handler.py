"""SCD Type 2 handler using Delta Lake MERGE for slowly changing dimensions."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from delta.tables import DeltaTable

from src.utils.logger import get_logger

logger = get_logger(__name__)


def apply_scd_type2(
    spark: SparkSession,
    new_data: DataFrame,
    target_path: str,
    key_column: str,
    tracked_columns: list,
) -> None:
    """Apply SCD Type 2 merge to a Delta table.

    For each incoming record:
    - If the key doesn't exist → INSERT with is_current=True
    - If the key exists and tracked columns changed →
      CLOSE the old record (is_current=False, effective_to=today)
      INSERT new version (is_current=True, effective_from=today)
    - If the key exists and nothing changed → no-op

    Args:
        spark: Active SparkSession.
        new_data: Incoming DataFrame with new/updated records.
        target_path: Delta table path.
        key_column: Business key column (e.g., 'customer_id').
        tracked_columns: Columns to track for changes (e.g., ['city', 'state']).
    """
    # Prepare incoming data with SCD columns
    staged = (
        new_data
        .withColumn("effective_from", F.current_date())
        .withColumn("effective_to", F.lit(None).cast("date"))
        .withColumn("is_current", F.lit(True))
        .withColumn("_ingestion_timestamp", F.current_timestamp())
    )

    if not DeltaTable.isDeltaTable(spark, target_path):
        # First load — write directly
        staged.write.format("delta").mode("overwrite").save(target_path)
        logger.info(f"SCD2 initial load: {staged.count()} rows to {target_path}")
        return

    target_table = DeltaTable.forPath(spark, target_path)

    # Build change detection condition
    change_conditions = [
        f"current.{col} <> staged.{col}" for col in tracked_columns
    ]
    change_condition = " OR ".join(change_conditions)

    # Identify records that have changed
    changes = (
        staged.alias("staged")
        .join(
            target_table.toDF().filter(F.col("is_current") == True).alias("current"),
            on=key_column,
            how="inner",
        )
        .filter(change_condition)
        .select(F.col(f"staged.{key_column}"))
    )

    changed_keys = [row[0] for row in changes.collect()]

    if not changed_keys:
        logger.info(f"SCD2 [{key_column}]: No changes detected")
        # Still insert any truly new records
        existing_keys = (
            target_table.toDF()
            .select(key_column)
            .distinct()
        )
        new_records = staged.join(existing_keys, on=key_column, how="left_anti")
        if new_records.count() > 0:
            new_records.write.format("delta").mode("append").save(target_path)
            logger.info(f"SCD2 [{key_column}]: {new_records.count()} new records inserted")
        return

    logger.info(f"SCD2 [{key_column}]: {len(changed_keys)} records changed")

    # Close old records for changed keys
    target_table.update(
        condition=(
            F.col(key_column).isin(changed_keys) & (F.col("is_current") == True)
        ),
        set={
            "is_current": F.lit(False),
            "effective_to": F.current_date(),
        },
    )

    # Insert new versions + any brand new records
    existing_keys = target_table.toDF().select(key_column).distinct()
    new_records = staged.join(existing_keys, on=key_column, how="left_anti")

    changed_new_versions = staged.filter(F.col(key_column).isin(changed_keys))

    records_to_insert = new_records.unionByName(changed_new_versions)
    records_to_insert.write.format("delta").mode("append").save(target_path)

    logger.info(
        f"SCD2 [{key_column}]: closed {len(changed_keys)} old records, "
        f"inserted {records_to_insert.count()} new versions"
    )
