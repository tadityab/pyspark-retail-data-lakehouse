"""Bronze → Silver transformations: dedup, type casting, cleansing, schema enforcement."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.utils.logger import get_logger

logger = get_logger(__name__)


def deduplicate(df: DataFrame, key_columns: list, order_column: str = "_ingestion_timestamp") -> DataFrame:
    """Remove duplicates keeping the latest record per key.

    Uses ROW_NUMBER window function — the production-standard approach
    that handles ties deterministically.
    """
    window = Window.partitionBy(*key_columns).orderBy(F.col(order_column).desc())
    deduped = (
        df
        .withColumn("_row_num", F.row_number().over(window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )
    original_count = df.count()
    deduped_count = deduped.count()
    logger.info(f"Dedup: {original_count} → {deduped_count} ({original_count - deduped_count} duplicates removed)")
    return deduped


def transform_orders(df: DataFrame) -> DataFrame:
    """Cleanse and type-cast Bronze orders to Silver schema."""
    return (
        df
        .withColumn("order_purchase_timestamp", F.to_timestamp("order_purchase_timestamp"))
        .withColumn("order_approved_at", F.to_timestamp("order_approved_at"))
        .withColumn("order_delivered_carrier_date", F.to_timestamp("order_delivered_carrier_date"))
        .withColumn("order_delivered_customer_date", F.to_timestamp("order_delivered_customer_date"))
        .withColumn("order_estimated_delivery_date", F.to_timestamp("order_estimated_delivery_date"))
        .withColumn("order_purchase_date", F.to_date("order_purchase_timestamp"))
        .withColumn("order_status", F.lower(F.trim("order_status")))
        .withColumn(
            "delivery_delay_days",
            F.datediff("order_delivered_customer_date", "order_estimated_delivery_date"),
        )
        .filter(F.col("order_id").isNotNull())
        .filter(F.col("order_purchase_timestamp").isNotNull())
    )


def transform_order_items(df: DataFrame) -> DataFrame:
    """Cleanse and type-cast Bronze order_items to Silver schema."""
    return (
        df
        .withColumn("order_item_id", F.col("order_item_id").cast("integer"))
        .withColumn("price", F.col("price").cast("double"))
        .withColumn("freight_value", F.col("freight_value").cast("double"))
        .withColumn("shipping_limit_date", F.to_timestamp("shipping_limit_date"))
        .withColumn("total_item_value", F.col("price") + F.col("freight_value"))
        .filter(F.col("order_id").isNotNull())
        .filter(F.col("product_id").isNotNull())
    )


def transform_order_payments(df: DataFrame) -> DataFrame:
    """Cleanse and type-cast Bronze order_payments to Silver schema."""
    return (
        df
        .withColumn("payment_sequential", F.col("payment_sequential").cast("integer"))
        .withColumn("payment_installments", F.col("payment_installments").cast("integer"))
        .withColumn("payment_value", F.col("payment_value").cast("double"))
        .withColumn("payment_type", F.lower(F.trim("payment_type")))
        .filter(F.col("order_id").isNotNull())
    )


def transform_products(df: DataFrame) -> DataFrame:
    """Cleanse and type-cast Bronze products to Silver schema."""
    return (
        df
        .withColumn("product_category_name", F.lower(F.trim("product_category_name")))
        .withColumn("product_name_length", F.col("product_name_lenght").cast("integer"))
        .withColumn("product_description_length", F.col("product_description_lenght").cast("integer"))
        .withColumn("product_photos_qty", F.col("product_photos_qty").cast("integer"))
        .withColumn("product_weight_g", F.col("product_weight_g").cast("integer"))
        .withColumn("product_length_cm", F.col("product_length_cm").cast("integer"))
        .withColumn("product_height_cm", F.col("product_height_cm").cast("integer"))
        .withColumn("product_width_cm", F.col("product_width_cm").cast("integer"))
        .drop("product_name_lenght", "product_description_lenght")
        .filter(F.col("product_id").isNotNull())
    )


def transform_reviews(df: DataFrame) -> DataFrame:
    """Cleanse and type-cast Bronze order_reviews to Silver schema."""
    return (
        df
        .withColumn("review_score", F.col("review_score").cast("integer"))
        .withColumn("review_creation_date", F.to_timestamp("review_creation_date"))
        .withColumn("review_answer_timestamp", F.to_timestamp("review_answer_timestamp"))
        .withColumn("review_comment_title", F.trim("review_comment_title"))
        .withColumn("review_comment_message", F.trim("review_comment_message"))
        .filter(F.col("review_id").isNotNull())
        .filter(F.col("order_id").isNotNull())
    )


# Registry for dispatching transformations by table name
TRANSFORM_REGISTRY = {
    "orders": transform_orders,
    "order_items": transform_order_items,
    "order_payments": transform_order_payments,
    "products": transform_products,
    "order_reviews": transform_reviews,
}


def run_bronze_to_silver(df: DataFrame, table_name: str, key_columns: list) -> DataFrame:
    """Execute full Bronze → Silver transformation for a table.

    Pipeline: deduplicate → transform → add metadata.
    """
    # Step 1: Deduplicate
    deduped = deduplicate(df, key_columns)

    # Step 2: Apply table-specific transformations
    transform_fn = TRANSFORM_REGISTRY.get(table_name)
    if transform_fn is None:
        logger.warning(f"No transformation registered for '{table_name}', passing through")
        transformed = deduped
    else:
        transformed = transform_fn(deduped)

    # Step 3: Add processing metadata
    result = transformed.withColumn("_ingestion_timestamp", F.current_timestamp())

    logger.info(f"Silver transformation [{table_name}]: {result.count()} rows output")
    return result
