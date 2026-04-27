"""Silver → Gold transformations: business aggregations and analytics tables."""

from pyspark.sql import SparkSession, DataFrame

from src.models.business_rules import (
    calculate_revenue_daily,
    calculate_customer_rfm,
    calculate_seller_performance,
)
from src.utils.io_helpers import read_parquet, write_delta, read_delta
from src.utils.logger import get_logger

logger = get_logger(__name__)


def build_revenue_daily(spark: SparkSession, silver_path: str, gold_path: str) -> DataFrame:
    """Build daily revenue Gold table from Silver layer.

    Joins orders + order_items + payments to compute daily KPIs.
    Uses broadcast join for the payments dimension.
    """
    orders = read_parquet(spark, f"{silver_path}/orders")
    order_items = read_parquet(spark, f"{silver_path}/order_items")

    revenue_df = calculate_revenue_daily(orders, order_items)

    write_delta(revenue_df, f"{gold_path}/revenue_daily", coalesce_num=4)
    logger.info(f"Gold revenue_daily: {revenue_df.count()} rows")
    return revenue_df


def build_customer_rfm(spark: SparkSession, silver_path: str, gold_path: str) -> DataFrame:
    """Build customer RFM segmentation Gold table.

    Computes Recency, Frequency, Monetary scores and assigns segments.
    """
    orders = read_parquet(spark, f"{silver_path}/orders")
    order_items = read_parquet(spark, f"{silver_path}/order_items")

    # Cache orders as it's used in multiple computations within RFM
    orders.cache()

    rfm_df = calculate_customer_rfm(orders, order_items)

    write_delta(rfm_df, f"{gold_path}/customer_rfm", coalesce_num=4)
    logger.info(f"Gold customer_rfm: {rfm_df.count()} rows")

    orders.unpersist()
    return rfm_df


def build_seller_performance(spark: SparkSession, silver_path: str,
                             gold_path: str) -> DataFrame:
    """Build seller performance scorecard Gold table.

    Joins orders + items + sellers + reviews for comprehensive metrics.
    Uses salted join on seller_id to handle skew (top sellers have
    disproportionately more orders).
    """
    orders = read_parquet(spark, f"{silver_path}/orders")
    order_items = read_parquet(spark, f"{silver_path}/order_items")
    sellers = read_parquet(spark, f"{silver_path}/sellers")
    reviews = read_parquet(spark, f"{silver_path}/order_reviews")

    # Broadcast sellers (small dimension ~3K rows)
    seller_perf = calculate_seller_performance(
        orders, order_items, F.broadcast(sellers), reviews
    )

    write_delta(seller_perf, f"{gold_path}/seller_performance", coalesce_num=4)
    logger.info(f"Gold seller_performance: {seller_perf.count()} rows")
    return seller_perf


def build_all_gold_tables(spark: SparkSession, silver_path: str, gold_path: str) -> dict:
    """Build all Gold layer tables from Silver.

    Returns dict with table name → row count.
    """
    results = {}

    logger.info("Building Gold: revenue_daily")
    rev = build_revenue_daily(spark, silver_path, gold_path)
    results["revenue_daily"] = rev.count()

    logger.info("Building Gold: customer_rfm")
    rfm = build_customer_rfm(spark, silver_path, gold_path)
    results["customer_rfm"] = rfm.count()

    logger.info("Building Gold: seller_performance")
    seller = build_seller_performance(spark, silver_path, gold_path)
    results["seller_performance"] = seller.count()

    logger.info(f"Gold layer complete: {results}")
    return results
