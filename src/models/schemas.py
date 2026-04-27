"""Schema definitions for all tables at every layer (Bronze, Silver, Gold).

StructType definitions enforce schema-on-write at the Silver layer,
catching schema drift before it corrupts downstream tables.
"""

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType, DateType, LongType, BooleanType,
)

# =============================================================================
# BRONZE SCHEMAS (permissive — all strings, cast later)
# =============================================================================

BRONZE_ORDERS = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("order_status", StringType(), True),
    StructField("order_purchase_timestamp", StringType(), True),
    StructField("order_approved_at", StringType(), True),
    StructField("order_delivered_carrier_date", StringType(), True),
    StructField("order_delivered_customer_date", StringType(), True),
    StructField("order_estimated_delivery_date", StringType(), True),
])

BRONZE_ORDER_ITEMS = StructType([
    StructField("order_id", StringType(), True),
    StructField("order_item_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("seller_id", StringType(), True),
    StructField("shipping_limit_date", StringType(), True),
    StructField("price", StringType(), True),
    StructField("freight_value", StringType(), True),
])

BRONZE_ORDER_PAYMENTS = StructType([
    StructField("order_id", StringType(), True),
    StructField("payment_sequential", StringType(), True),
    StructField("payment_type", StringType(), True),
    StructField("payment_installments", StringType(), True),
    StructField("payment_value", StringType(), True),
])

BRONZE_ORDER_REVIEWS = StructType([
    StructField("review_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("review_score", StringType(), True),
    StructField("review_comment_title", StringType(), True),
    StructField("review_comment_message", StringType(), True),
    StructField("review_creation_date", StringType(), True),
    StructField("review_answer_timestamp", StringType(), True),
])

BRONZE_CUSTOMERS = StructType([
    StructField("customer_id", StringType(), True),
    StructField("customer_unique_id", StringType(), True),
    StructField("customer_zip_code_prefix", StringType(), True),
    StructField("customer_city", StringType(), True),
    StructField("customer_state", StringType(), True),
])

BRONZE_SELLERS = StructType([
    StructField("seller_id", StringType(), True),
    StructField("seller_zip_code_prefix", StringType(), True),
    StructField("seller_city", StringType(), True),
    StructField("seller_state", StringType(), True),
])

BRONZE_PRODUCTS = StructType([
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

BRONZE_GEOLOCATION = StructType([
    StructField("geolocation_zip_code_prefix", StringType(), True),
    StructField("geolocation_lat", StringType(), True),
    StructField("geolocation_lng", StringType(), True),
    StructField("geolocation_city", StringType(), True),
    StructField("geolocation_state", StringType(), True),
])

# =============================================================================
# SILVER SCHEMAS (strongly typed — enforced on write)
# =============================================================================

SILVER_ORDERS = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("order_status", StringType(), True),
    StructField("order_purchase_timestamp", TimestampType(), False),
    StructField("order_approved_at", TimestampType(), True),
    StructField("order_delivered_carrier_date", TimestampType(), True),
    StructField("order_delivered_customer_date", TimestampType(), True),
    StructField("order_estimated_delivery_date", TimestampType(), True),
    StructField("order_purchase_date", DateType(), True),
    StructField("delivery_delay_days", IntegerType(), True),
    StructField("_ingestion_timestamp", TimestampType(), True),
])

SILVER_ORDER_ITEMS = StructType([
    StructField("order_id", StringType(), False),
    StructField("order_item_id", IntegerType(), False),
    StructField("product_id", StringType(), False),
    StructField("seller_id", StringType(), False),
    StructField("shipping_limit_date", TimestampType(), True),
    StructField("price", DoubleType(), True),
    StructField("freight_value", DoubleType(), True),
    StructField("total_item_value", DoubleType(), True),
    StructField("_ingestion_timestamp", TimestampType(), True),
])

SILVER_ORDER_PAYMENTS = StructType([
    StructField("order_id", StringType(), False),
    StructField("payment_sequential", IntegerType(), False),
    StructField("payment_type", StringType(), True),
    StructField("payment_installments", IntegerType(), True),
    StructField("payment_value", DoubleType(), True),
    StructField("_ingestion_timestamp", TimestampType(), True),
])

SILVER_CUSTOMERS = StructType([
    StructField("customer_id", StringType(), False),
    StructField("customer_unique_id", StringType(), True),
    StructField("customer_zip_code_prefix", StringType(), True),
    StructField("customer_city", StringType(), True),
    StructField("customer_state", StringType(), True),
    StructField("effective_from", DateType(), True),
    StructField("effective_to", DateType(), True),
    StructField("is_current", BooleanType(), True),
    StructField("_ingestion_timestamp", TimestampType(), True),
])

SILVER_SELLERS = StructType([
    StructField("seller_id", StringType(), False),
    StructField("seller_zip_code_prefix", StringType(), True),
    StructField("seller_city", StringType(), True),
    StructField("seller_state", StringType(), True),
    StructField("effective_from", DateType(), True),
    StructField("effective_to", DateType(), True),
    StructField("is_current", BooleanType(), True),
    StructField("_ingestion_timestamp", TimestampType(), True),
])

SILVER_PRODUCTS = StructType([
    StructField("product_id", StringType(), False),
    StructField("product_category_name", StringType(), True),
    StructField("product_name_length", IntegerType(), True),
    StructField("product_description_length", IntegerType(), True),
    StructField("product_photos_qty", IntegerType(), True),
    StructField("product_weight_g", IntegerType(), True),
    StructField("product_length_cm", IntegerType(), True),
    StructField("product_height_cm", IntegerType(), True),
    StructField("product_width_cm", IntegerType(), True),
    StructField("_ingestion_timestamp", TimestampType(), True),
])

# =============================================================================
# GOLD SCHEMAS (analytics-ready)
# =============================================================================

GOLD_REVENUE_DAILY = StructType([
    StructField("order_purchase_date", DateType(), False),
    StructField("total_orders", LongType(), True),
    StructField("total_revenue", DoubleType(), True),
    StructField("total_freight", DoubleType(), True),
    StructField("avg_order_value", DoubleType(), True),
    StructField("total_items", LongType(), True),
    StructField("unique_customers", LongType(), True),
    StructField("unique_sellers", LongType(), True),
])

GOLD_CUSTOMER_RFM = StructType([
    StructField("customer_unique_id", StringType(), False),
    StructField("recency_days", IntegerType(), True),
    StructField("frequency", LongType(), True),
    StructField("monetary", DoubleType(), True),
    StructField("r_score", IntegerType(), True),
    StructField("f_score", IntegerType(), True),
    StructField("m_score", IntegerType(), True),
    StructField("rfm_segment", StringType(), True),
    StructField("_computed_at", TimestampType(), True),
])

GOLD_SELLER_PERFORMANCE = StructType([
    StructField("seller_id", StringType(), False),
    StructField("seller_city", StringType(), True),
    StructField("seller_state", StringType(), True),
    StructField("total_orders", LongType(), True),
    StructField("total_revenue", DoubleType(), True),
    StructField("total_products_sold", LongType(), True),
    StructField("avg_review_score", DoubleType(), True),
    StructField("avg_delivery_delay_days", DoubleType(), True),
    StructField("on_time_delivery_rate", DoubleType(), True),
    StructField("_computed_at", TimestampType(), True),
])

# Schema registry for easy lookup
SCHEMA_REGISTRY = {
    "bronze": {
        "orders": BRONZE_ORDERS,
        "order_items": BRONZE_ORDER_ITEMS,
        "order_payments": BRONZE_ORDER_PAYMENTS,
        "order_reviews": BRONZE_ORDER_REVIEWS,
        "customers": BRONZE_CUSTOMERS,
        "sellers": BRONZE_SELLERS,
        "products": BRONZE_PRODUCTS,
        "geolocation": BRONZE_GEOLOCATION,
    },
    "silver": {
        "orders": SILVER_ORDERS,
        "order_items": SILVER_ORDER_ITEMS,
        "order_payments": SILVER_ORDER_PAYMENTS,
        "customers": SILVER_CUSTOMERS,
        "sellers": SILVER_SELLERS,
        "products": SILVER_PRODUCTS,
    },
    "gold": {
        "revenue_daily": GOLD_REVENUE_DAILY,
        "customer_rfm": GOLD_CUSTOMER_RFM,
        "seller_performance": GOLD_SELLER_PERFORMANCE,
    },
}


def get_schema(layer: str, table: str) -> StructType:
    """Retrieve schema by layer and table name."""
    if layer not in SCHEMA_REGISTRY:
        raise ValueError(f"Unknown layer: {layer}. Available: {list(SCHEMA_REGISTRY.keys())}")
    if table not in SCHEMA_REGISTRY[layer]:
        raise ValueError(
            f"Unknown table: {table} in layer {layer}. "
            f"Available: {list(SCHEMA_REGISTRY[layer].keys())}"
        )
    return SCHEMA_REGISTRY[layer][table]
