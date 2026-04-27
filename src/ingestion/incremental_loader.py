"""Incremental loader — watermark-based ingestion for new/changed data."""

import json
import os
from datetime import datetime, timezone

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.utils.logger import get_logger

logger = get_logger(__name__)


class WatermarkManager:
    """Manages high-watermark state for incremental loading.

    Stores the last processed timestamp per table in a JSON file.
    Production systems would use a metadata database; this file-based
    approach demonstrates the pattern without external dependencies.
    """

    def __init__(self, watermark_path: str):
        self.watermark_path = watermark_path
        self._watermarks = self._load()

    def _load(self) -> dict:
        """Load watermarks from disk."""
        if os.path.exists(self.watermark_path):
            with open(self.watermark_path, "r") as f:
                return json.load(f)
        return {}

    def _save(self):
        """Persist watermarks to disk."""
        os.makedirs(os.path.dirname(self.watermark_path), exist_ok=True)
        with open(self.watermark_path, "w") as f:
            json.dump(self._watermarks, f, indent=2, default=str)

    def get_watermark(self, table_name: str) -> str | None:
        """Get the last processed timestamp for a table."""
        return self._watermarks.get(table_name)

    def update_watermark(self, table_name: str, timestamp: str):
        """Update the watermark for a table after successful processing."""
        self._watermarks[table_name] = timestamp
        self._watermarks[f"{table_name}_updated_at"] = datetime.now(timezone.utc).isoformat()
        self._save()
        logger.info(f"Watermark updated: {table_name} = {timestamp}")


def load_incremental(
    source_df: DataFrame,
    watermark_manager: WatermarkManager,
    table_name: str,
    watermark_column: str,
    lookback_hours: int = 6,
) -> DataFrame:
    """Filter a DataFrame to only new records since last watermark.

    Applies a lookback window to catch late-arriving data.

    Args:
        spark: Active SparkSession.
        source_df: Full source DataFrame.
        watermark_manager: WatermarkManager instance.
        table_name: Table identifier for watermark lookup.
        watermark_column: Column used for incremental filtering.
        lookback_hours: Hours to look back beyond the watermark (safety overlap).

    Returns:
        Filtered DataFrame containing only new/updated records.
    """
    last_watermark = watermark_manager.get_watermark(table_name)

    if last_watermark is None:
        logger.info(f"No watermark found for {table_name} — full load")
        new_watermark = (
            source_df
            .agg(F.max(F.col(watermark_column)).alias("max_ts"))
            .collect()[0]["max_ts"]
        )
        watermark_manager.update_watermark(table_name, str(new_watermark))
        return source_df

    # Apply lookback window for late arrivals
    adjusted_watermark = (
        F.to_timestamp(F.lit(last_watermark)) - F.expr(f"INTERVAL {lookback_hours} HOURS")
    )

    incremental_df = source_df.filter(
        F.col(watermark_column) > adjusted_watermark
    )

    row_count = incremental_df.count()
    logger.info(
        f"Incremental load [{table_name}]: {row_count} new rows "
        f"(watermark: {last_watermark}, lookback: {lookback_hours}h)"
    )

    # Update watermark to max value in this batch
    if row_count > 0:
        new_watermark = (
            incremental_df
            .agg(F.max(F.col(watermark_column)).alias("max_ts"))
            .collect()[0]["max_ts"]
        )
        watermark_manager.update_watermark(table_name, str(new_watermark))

    return incremental_df
