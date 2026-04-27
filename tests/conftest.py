"""Shared test fixtures — local SparkSession and sample DataFrames."""

import os
import sys
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


@pytest.fixture(scope="session")
def spark():
    """Create a local SparkSession for testing.

    Session-scoped: one SparkSession shared across all tests for speed.
    """
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("RetailDataLakehouse-Tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.memory", "2g")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def sample_orders(spark):
    """Sample orders DataFrame for testing."""
    data = [
        ("ord_001", "cust_001", "delivered", "2023-01-15 10:30:00", "2023-01-15 12:00:00",
         "2023-01-17 08:00:00", "2023-01-20 14:00:00", "2023-01-22 00:00:00"),
        ("ord_002", "cust_002", "delivered", "2023-01-16 09:00:00", "2023-01-16 10:00:00",
         "2023-01-18 09:00:00", "2023-01-19 16:00:00", "2023-01-25 00:00:00"),
        ("ord_003", "cust_001", "shipped", "2023-02-01 14:00:00", "2023-02-01 15:00:00",
         None, None, "2023-02-10 00:00:00"),
        ("ord_004", "cust_003", "canceled", "2023-02-05 11:00:00", None,
         None, None, "2023-02-12 00:00:00"),
        # Duplicate order for dedup testing
        ("ord_001", "cust_001", "delivered", "2023-01-15 10:30:00", "2023-01-15 12:00:00",
         "2023-01-17 08:00:00", "2023-01-20 14:00:00", "2023-01-22 00:00:00"),
    ]

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

    from pyspark.sql import functions as F
    return spark.createDataFrame(data, schema).withColumn(
        "_ingestion_timestamp", F.current_timestamp()
    )


@pytest.fixture
def sample_order_items(spark):
    """Sample order items DataFrame for testing."""
    data = [
        ("ord_001", "1", "prod_001", "seller_001", "2023-01-20 00:00:00", "100.00", "15.50"),
        ("ord_001", "2", "prod_002", "seller_002", "2023-01-20 00:00:00", "50.00", "10.00"),
        ("ord_002", "1", "prod_001", "seller_001", "2023-01-22 00:00:00", "100.00", "15.50"),
        ("ord_003", "1", "prod_003", "seller_001", "2023-02-05 00:00:00", "200.00", "25.00"),
    ]

    schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("order_item_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("seller_id", StringType(), True),
        StructField("shipping_limit_date", StringType(), True),
        StructField("price", StringType(), True),
        StructField("freight_value", StringType(), True),
    ])

    from pyspark.sql import functions as F
    return spark.createDataFrame(data, schema).withColumn(
        "_ingestion_timestamp", F.current_timestamp()
    )


@pytest.fixture
def sample_customers(spark):
    """Sample customers DataFrame for testing."""
    data = [
        ("cust_001", "unique_001", "01001", "sao paulo", "SP"),
        ("cust_002", "unique_002", "02002", "rio de janeiro", "RJ"),
        ("cust_003", "unique_003", "03003", "belo horizonte", "MG"),
    ]

    schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("customer_unique_id", StringType(), True),
        StructField("customer_zip_code_prefix", StringType(), True),
        StructField("customer_city", StringType(), True),
        StructField("customer_state", StringType(), True),
    ])

    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_products(spark):
    """Sample products DataFrame for testing."""
    data = [
        ("prod_001", "electronics", "20", "100", "2", "500", "30", "10", "20"),
        ("prod_002", "furniture", "15", "80", "3", "10000", "100", "50", "60"),
        ("prod_003", "toys", "10", "50", "1", "200", "20", "15", "10"),
    ]

    schema = StructType([
        StructField("product_id", StringType(), True),
        StructField("product_category_name", StringType(), True),
        StructField("product_name_lenght", StringType(), True),
        StructField("product_description_lenght", StringType(), True),
        StructField("product_photos_qty", StringType(), True),
        StructField("product_weight_g", StringType(), True),
        StructField("product_length_cm", StringType(), True),
        StructField("product_height_cm", StringType(), True),
        StructField("product_width_cm", StringType(), True),
    ])

    return spark.createDataFrame(data, schema)
