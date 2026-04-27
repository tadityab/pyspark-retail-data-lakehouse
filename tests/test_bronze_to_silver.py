"""Tests for Bronze → Silver transformations."""

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
from src.transformations.bronze_to_silver import (
    deduplicate,
    transform_orders,
    transform_order_items,
    transform_products,
    run_bronze_to_silver,
)


class TestDeduplicate:
    """Tests for the deduplication logic."""

    def test_removes_duplicates(self, spark, sample_orders):
        result = deduplicate(sample_orders, ["order_id"])
        assert result.count() == 4  # 5 rows → 4 unique order_ids

    def test_keeps_latest_record(self, spark, sample_orders):
        result = deduplicate(sample_orders, ["order_id"])
        assert result.filter(F.col("order_id") == "ord_001").count() == 1

    def test_no_duplicates_passes_through(self, spark, sample_order_items):
        result = deduplicate(sample_order_items, ["order_id", "order_item_id"])
        assert result.count() == sample_order_items.count()


class TestTransformOrders:
    """Tests for orders transformation."""

    def test_timestamps_are_parsed(self, spark, sample_orders):
        result = transform_orders(sample_orders)
        schema_types = {f.name: f.dataType.simpleString() for f in result.schema.fields}
        assert schema_types["order_purchase_timestamp"] == "timestamp"

    def test_date_column_added(self, spark, sample_orders):
        result = transform_orders(sample_orders)
        assert "order_purchase_date" in result.columns

    def test_delivery_delay_calculated(self, spark, sample_orders):
        result = transform_orders(sample_orders)
        delayed = result.filter(F.col("order_id") == "ord_002").collect()
        assert len(delayed) > 0
        # Delivered 2023-01-19, estimated 2023-01-25 → -6 days (early)
        assert delayed[0]["delivery_delay_days"] == -6

    def test_null_order_id_filtered(self, spark):
        data = [(
            None,
            "cust_x",
            "delivered",
            "2023-01-01 00:00:00",
            None,
            None,
            None,
            None,
        )]
        schema = StructType([
            StructField("order_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("order_status", StringType(), True),
            StructField("order_purchase_timestamp", StringType(), True),
            StructField("order_approved_at", StringType(), True),
            StructField("order_delivered_carrier_date", StringType(), True),
            StructField("order_delivered_customer_date", StringType(), True),
            StructField("order_estimated_delivery_date", StringType(), True),
        ])
        df = spark.createDataFrame(data, schema=schema)
        result = transform_orders(df)
        assert result.count() == 0

    def test_status_lowered_and_trimmed(self, spark, sample_orders):
        result = transform_orders(sample_orders)
        statuses = [row["order_status"] for row in result.collect()]
        assert all(s == s.lower().strip() for s in statuses)


class TestTransformOrderItems:
    """Tests for order items transformation."""

    def test_price_cast_to_double(self, spark, sample_order_items):
        result = transform_order_items(sample_order_items)
        schema_types = {f.name: f.dataType.simpleString() for f in result.schema.fields}
        assert schema_types["price"] == "double"

    def test_total_item_value_calculated(self, spark, sample_order_items):
        result = transform_order_items(sample_order_items)
        row = result.filter(F.col("order_id") == "ord_001").filter(
            F.col("order_item_id") == 1
        ).collect()[0]
        assert row["total_item_value"] == pytest.approx(115.50)  # 100 + 15.50


class TestTransformProducts:
    """Tests for products transformation."""

    def test_column_rename_typo_fix(self, spark, sample_products):
        result = transform_products(sample_products)
        assert "product_name_length" in result.columns
        assert "product_name_lenght" not in result.columns

    def test_numeric_fields_cast(self, spark, sample_products):
        result = transform_products(sample_products)
        schema_types = {f.name: f.dataType.simpleString() for f in result.schema.fields}
        assert schema_types["product_weight_g"] == "int"


class TestRunBronzeToSilver:
    """Integration tests for the full Bronze → Silver pipeline."""

    def test_full_pipeline_orders(self, spark, sample_orders):
        result = run_bronze_to_silver(sample_orders, "orders", ["order_id"])
        assert result.count() > 0
        assert "_ingestion_timestamp" in result.columns

    def test_full_pipeline_unknown_table(self, spark, sample_orders):
        # Unknown tables should pass through with a warning
        result = run_bronze_to_silver(sample_orders, "unknown_table", ["order_id"])
        assert result.count() > 0
