"""Business rules for revenue calculations, RFM scoring, and seller metrics."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def calculate_revenue_daily(orders_df: DataFrame, order_items_df: DataFrame,
                            ) -> DataFrame:
    """Compute daily revenue aggregates from Silver layer tables.

    Joins orders with items and payments to produce per-day KPIs:
    total orders, revenue, freight, AOV, item count, unique customers/sellers.
    """
    # Join orders with items
    orders_items = (
        orders_df
        .filter(F.col("order_status") == "delivered")
        .join(order_items_df, on="order_id", how="inner")
    )

    daily_revenue = (
        orders_items
        .groupBy("order_purchase_date")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.sum("total_item_value").alias("total_revenue"),
            F.sum("freight_value").alias("total_freight"),
            F.round(
                F.sum("total_item_value") / F.countDistinct("order_id"), 2
            ).alias("avg_order_value"),
            F.count("order_item_id").alias("total_items"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.countDistinct("seller_id").alias("unique_sellers"),
        )
        .orderBy("order_purchase_date")
    )
    return daily_revenue


def calculate_customer_rfm(orders_df: DataFrame, order_items_df: DataFrame,
                           reference_date=None) -> DataFrame:
    """Compute RFM (Recency, Frequency, Monetary) scores per customer.

    Uses window-based ntile scoring (1-5 scale) and assigns segments.
    """
    if reference_date is None:
        reference_date = orders_df.agg(F.max("order_purchase_timestamp")).collect()[0][0]

    # Base metrics
    rfm_base = (
        orders_df
        .filter(F.col("order_status") == "delivered")
        .join(order_items_df, on="order_id", how="inner")
        .groupBy("customer_id")
        .agg(
            F.datediff(F.lit(reference_date), F.max("order_purchase_timestamp")).alias("recency_days"),
            F.countDistinct("order_id").alias("frequency"),
            F.round(F.sum("total_item_value"), 2).alias("monetary"),
        )
    )

    # Score using ntile (quintiles)
    recency_window = Window.orderBy(F.col("recency_days").desc())  # lower recency = better
    frequency_window = Window.orderBy("frequency")
    monetary_window = Window.orderBy("monetary")

    rfm_scored = (
        rfm_base
        .withColumn("r_score", F.ntile(5).over(recency_window))
        .withColumn("f_score", F.ntile(5).over(frequency_window))
        .withColumn("m_score", F.ntile(5).over(monetary_window))
    )

    # Segment assignment
    rfm_segmented = rfm_scored.withColumn(
        "rfm_segment",
        F.when(
            (F.col("r_score") >= 4) & (F.col("f_score") >= 4) & (F.col("m_score") >= 4),
            "Champions"
        ).when(
            (F.col("r_score") >= 3) & (F.col("f_score") >= 3),
            "Loyal Customers"
        ).when(
            (F.col("r_score") >= 4) & (F.col("f_score") <= 2),
            "New Customers"
        ).when(
            (F.col("r_score") <= 2) & (F.col("f_score") >= 3),
            "At Risk"
        ).when(
            (F.col("r_score") <= 2) & (F.col("f_score") <= 2),
            "Lost"
        ).otherwise("Needs Attention")
    ).withColumn("_computed_at", F.current_timestamp())

    return rfm_segmented


def calculate_seller_performance(orders_df: DataFrame, order_items_df: DataFrame,
                                 sellers_df: DataFrame,
                                 reviews_df: DataFrame = None) -> DataFrame:
    """Compute seller performance scorecard.

    Metrics: total orders, revenue, products sold, avg review score,
    delivery delay, on-time delivery rate.
    """
    # Base seller metrics from order items
    seller_base = (
        orders_df
        .filter(F.col("order_status") == "delivered")
        .join(order_items_df, on="order_id", how="inner")
        .groupBy("seller_id")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.round(F.sum("total_item_value"), 2).alias("total_revenue"),
            F.count("order_item_id").alias("total_products_sold"),
            F.round(F.avg("delivery_delay_days"), 1).alias("avg_delivery_delay_days"),
            F.round(
                F.sum(F.when(F.col("delivery_delay_days") <= 0, 1).otherwise(0))
                / F.count("order_id"),
                3,
            ).alias("on_time_delivery_rate"),
        )
    )

    # Add review scores if available
    if reviews_df is not None:
        review_agg = (
            reviews_df
            .join(order_items_df.select("order_id", "seller_id"), on="order_id", how="inner")
            .groupBy("seller_id")
            .agg(F.round(F.avg("review_score"), 2).alias("avg_review_score"))
        )
        seller_base = seller_base.join(review_agg, on="seller_id", how="left")
    else:
        seller_base = seller_base.withColumn("avg_review_score", F.lit(None).cast("double"))

    # Enrich with seller location
    result = (
        seller_base
        .join(
            sellers_df.filter(F.col("is_current") == True).select(
                "seller_id", "seller_city", "seller_state"
            ),
            on="seller_id",
            how="left",
        )
        .withColumn("_computed_at", F.current_timestamp())
    )

    return result
