# Raw Data

Download the Olist Brazilian E-Commerce dataset from Kaggle:
https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

## Setup

1. Download and extract the dataset
2. Place all CSV files in this directory:

```
data/raw/olist/
├── olist_orders_dataset.csv
├── olist_order_items_dataset.csv
├── olist_order_payments_dataset.csv
├── olist_order_reviews_dataset.csv
├── olist_customers_dataset.csv
├── olist_sellers_dataset.csv
├── olist_products_dataset.csv
└── olist_geolocation_dataset.csv
```

## Alternative: Use Sample Data

For quick testing without downloading, use the sample data in `data/sample/`.
Run `make test` — tests use built-in fixtures, no download needed.

## Generate Synthetic Data at Scale

To test at production scale (millions of rows):

```bash
python data/synthetic/data_generator.py --rows 10000000 --output data/raw/synthetic
```
