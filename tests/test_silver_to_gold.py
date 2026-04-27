"""Tests for Silver → Gold business rule calculations."""

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType,
    TimestampType, DateType, BooleanType,
)

from src.models.business_rules import calculate_revenue_daily, calculate_customer_rfm


@pytest.fixture
def silver_orders(spark):
    """Silver-layer orders with typed columns."""
    data = [
        ("ord_001", "cust_001", "delivered", "2023-01-15", -2),
        ("ord_002", "cust_002", "delivered", "2023-01-15", 3),
        ("ord_003", "cust_001", "delivered", "2023-01-16", 0),
        ("ord_004", "cust_003", "canceled", "2023-01-16", None),
    ]
    schema = StructType([
        StructField("order_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("order_status", StringType()),
        StructField("order_purchase_date", StringType()),
        StructField("delivery_delay_days", IntegerType()),
    ])
    df = spark.createDataFrame(data, schema)
    return df.withColumn("order_purchase_date", F.to_date("order_purchase_date")) \
             .withColumn("order_purchase_timestamp", F.to_timestamp(F.lit("2023-01-15 10:00:00")))


@pytest.fixture
def silver_order_items(spark):
    """Silver-layer order items with typed columns."""
    data = [
        ("ord_001", 1, "prod_001", "seller_001", 100.0, 15.0, 115.0),
        ("ord_001", 2, "prod_002", "seller_002", 50.0, 10.0, 60.0),
        ("ord_002", 1, "prod_001", "seller_001", 100.0, 15.0, 115.0),
        ("ord_003", 1, "prod_003", "seller_001", 200.0, 25.0, 225.0),
    ]
    schema = StructType([
        StructField("order_id", StringType()),
        StructField("order_item_id", IntegerType()),
        StructField("product_id", StringType()),
        StructField("seller_id", StringType()),
        StructField("price", DoubleType()),
        StructField("freight_value", DoubleType()),
        StructField("total_item_value", DoubleType()),
    ])
    return spark.createDataFrame(data, schema)


class TestRevenueDailyCalculation:
    """Tests for daily revenue aggregation."""

    def test_revenue_calculation(self, spark, silver_orders, silver_order_items):
        result = calculate_revenue_daily(silver_orders, silver_order_items)
        assert result.count() > 0

    def test_excludes_canceled_orders(self, spark, silver_orders, silver_order_items):
        calculate_revenue_daily(silver_orders, silver_order_items)
        # ord_004 is canceled, should not appear
        order_ids_in_result = silver_orders.filter(
            F.col("order_status") == "delivered"
        ).select("order_id").distinct()
        assert order_ids_in_result.count() == 3


class TestCustomerRFM:
    """Tests for RFM scoring."""

    def test_rfm_produces_segments(self, spark, silver_orders, silver_order_items):
        result = calculate_customer_rfm(silver_orders, silver_order_items)
        assert "rfm_segment" in result.columns
        assert result.count() > 0

    def test_rfm_scores_in_range(self, spark, silver_orders, silver_order_items):
        result = calculate_customer_rfm(silver_orders, silver_order_items)
        scores = result.select("r_score", "f_score", "m_score").collect()
        for row in scores:
            assert 1 <= row["r_score"] <= 5
            assert 1 <= row["f_score"] <= 5
            assert 1 <= row["m_score"] <= 5
