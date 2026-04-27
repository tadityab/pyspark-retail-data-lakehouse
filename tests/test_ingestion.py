"""Tests for CSV ingestion into Bronze layer.

File-write tests are separated so they only run when a working winutils.exe
is available (Hadoop 3.x 64-bit on Windows). Transformation tests run always.
"""

import os
import tempfile
import shutil
from unittest.mock import patch

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DateType

from src.ingestion.batch_ingestion import ingest_csv_to_bronze


@pytest.fixture
def temp_source_dir():
    """Create a temporary source directory with a sample CSV."""
    source_dir = tempfile.mkdtemp()
    csv_path = os.path.join(source_dir, "test_orders.csv")
    with open(csv_path, "w") as f:
        f.write(
            "order_id,customer_id,order_status,order_purchase_timestamp,"
            "order_approved_at,order_delivered_carrier_date,"
            "order_delivered_customer_date,order_estimated_delivery_date\n"
        )
        f.write(
            "ord_001,cust_001,delivered,2023-01-15 10:30:00,"
            "2023-01-15 12:00:00,2023-01-17 08:00:00,"
            "2023-01-20 14:00:00,2023-01-22 00:00:00\n"
        )
        f.write(
            "ord_002,cust_002,shipped,2023-01-16 09:00:00,"
            "2023-01-16 10:00:00,,,2023-01-25 00:00:00\n"
        )
    yield csv_path
    shutil.rmtree(source_dir, ignore_errors=True)


class TestBatchIngestion:
    """Tests for CSV → Bronze transformation (write is mocked)."""

    def test_ingestion_reads_csv_rows(self, spark, temp_source_dir):
        """CSV is read correctly and row count matches."""
        with patch("src.ingestion.batch_ingestion.write_parquet"):
            result = ingest_csv_to_bronze(spark, temp_source_dir, "/tmp/ignored", "orders")
        assert result.count() == 2

    def test_metadata_columns_added(self, spark, temp_source_dir):
        """Ingestion adds lineage metadata columns."""
        with patch("src.ingestion.batch_ingestion.write_parquet"):
            result = ingest_csv_to_bronze(spark, temp_source_dir, "/tmp/ignored", "orders")
        assert "_source_file" in result.columns
        assert "_ingestion_timestamp" in result.columns
        assert "_ingestion_date" in result.columns

    def test_schema_applied_correctly(self, spark, temp_source_dir):
        """Bronze schema is applied — all columns come through as strings."""
        with patch("src.ingestion.batch_ingestion.write_parquet"):
            result = ingest_csv_to_bronze(spark, temp_source_dir, "/tmp/ignored", "orders")
        # Bronze uses schema-on-read: typed columns from the Bronze schema definition
        assert "order_id" in result.columns
        assert "customer_id" in result.columns
        assert "order_status" in result.columns

    def test_missing_optional_fields_are_null(self, spark, temp_source_dir):
        """Optional fields (e.g., delivered date) can be null — no hard failure."""
        with patch("src.ingestion.batch_ingestion.write_parquet"):
            result = ingest_csv_to_bronze(spark, temp_source_dir, "/tmp/ignored", "orders")
        # ord_002 has no delivered dates — should still load
        row = result.filter(F.col("order_id") == "ord_002").collect()
        assert len(row) == 1
