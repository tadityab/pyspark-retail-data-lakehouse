"""Synthetic data generator — scale Olist patterns to millions of rows.

Usage:
    python data/synthetic/data_generator.py --rows 10000000 --output data/raw/synthetic
"""

import argparse
import os
import sys
import random
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, TimestampType,
)


def create_spark():
    return (
        SparkSession.builder
        .master("local[*]")
        .appName("SyntheticDataGenerator")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )


def generate_orders(spark, num_rows: int, num_customers: int = 50000,
                    start_date: str = "2020-01-01", end_date: str = "2024-12-31"):
    """Generate synthetic orders with realistic patterns."""
    statuses = ["delivered"] * 85 + ["shipped"] * 5 + ["canceled"] * 5 + \
               ["invoiced"] * 2 + ["processing"] * 2 + ["unavailable"] * 1

    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    date_range = (end - start).days

    rows = []
    for i in range(num_rows):
        order_id = f"ord_{i:012d}"
        customer_id = f"cust_{random.randint(1, num_customers):08d}"
        status = random.choice(statuses)

        purchase_offset = random.randint(0, date_range)
        purchase_ts = start + timedelta(
            days=purchase_offset,
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
        )

        # Late arrival simulation (5% arrive 1-3 days late)
        if random.random() < 0.05:
            purchase_ts -= timedelta(days=random.randint(1, 3))

        rows.append((
            order_id, customer_id, status,
            purchase_ts.strftime("%Y-%m-%d %H:%M:%S"),
        ))

    schema = StructType([
        StructField("order_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("order_status", StringType()),
        StructField("order_purchase_timestamp", StringType()),
    ])

    return spark.createDataFrame(rows, schema)


def generate_order_items(spark, orders_df, avg_items_per_order: float = 1.5,
                         num_products: int = 10000, num_sellers: int = 3000):
    """Generate order items with skewed seller distribution."""
    order_ids = [row["order_id"] for row in orders_df.select("order_id").collect()]

    rows = []
    for order_id in order_ids:
        num_items = max(1, int(random.gauss(avg_items_per_order, 0.8)))
        for item_id in range(1, num_items + 1):
            product_id = f"prod_{random.randint(1, num_products):06d}"

            # Skew: top 1% sellers get 40% of orders
            if random.random() < 0.40:
                seller_id = f"seller_{random.randint(1, 30):06d}"
            else:
                seller_id = f"seller_{random.randint(31, num_sellers):06d}"

            price = round(random.uniform(5.0, 2000.0), 2)
            freight = round(random.uniform(5.0, 80.0), 2)

            rows.append((order_id, item_id, product_id, seller_id, price, freight))

    schema = StructType([
        StructField("order_id", StringType()),
        StructField("order_item_id", IntegerType()),
        StructField("product_id", StringType()),
        StructField("seller_id", StringType()),
        StructField("price", DoubleType()),
        StructField("freight_value", DoubleType()),
    ])

    return spark.createDataFrame(rows, schema)


def main():
    parser = argparse.ArgumentParser(description="Generate synthetic e-commerce data")
    parser.add_argument("--rows", type=int, default=100000, help="Number of orders")
    parser.add_argument("--output", default="data/raw/synthetic", help="Output path")
    args = parser.parse_args()

    print(f"Generating {args.rows:,} synthetic orders...")
    spark = create_spark()

    orders = generate_orders(spark, args.rows)
    orders.write.mode("overwrite").csv(f"{args.output}/orders", header=True)
    print(f"Orders: {orders.count():,} rows")

    items = generate_order_items(spark, orders)
    items.write.mode("overwrite").csv(f"{args.output}/order_items", header=True)
    print(f"Order items: {items.count():,} rows")

    print(f"Synthetic data written to {args.output}")
    spark.stop()


if __name__ == "__main__":
    main()
