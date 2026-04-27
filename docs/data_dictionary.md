# Data Dictionary

## Bronze Tables

### orders
- order_id: string
- customer_id: string
- order_status: string
- order_purchase_timestamp: string
- order_approved_at: string
- order_delivered_carrier_date: string
- order_delivered_customer_date: string
- order_estimated_delivery_date: string
- _source_file: string
- _ingestion_timestamp: timestamp
- _ingestion_date: date

### order_items
- order_id: string
- order_item_id: string
- product_id: string
- seller_id: string
- shipping_limit_date: string
- price: string
- freight_value: string
- _source_file: string
- _ingestion_timestamp: timestamp
- _ingestion_date: date

### order_payments
- order_id: string
- payment_sequential: string
- payment_type: string
- payment_installments: string
- payment_value: string
- _source_file: string
- _ingestion_timestamp: timestamp
- _ingestion_date: date

## Silver Tables

### orders
- order_id: string (PK)
- customer_id: string
- order_status: string
- order_purchase_timestamp: timestamp
- order_approved_at: timestamp
- order_delivered_carrier_date: timestamp
- order_delivered_customer_date: timestamp
- order_estimated_delivery_date: timestamp
- order_purchase_date: date
- delivery_delay_days: integer
- _ingestion_timestamp: timestamp

### order_items
- order_id: string (PK part)
- order_item_id: integer (PK part)
- product_id: string
- seller_id: string
- shipping_limit_date: timestamp
- price: double
- freight_value: double
- total_item_value: double
- _ingestion_timestamp: timestamp

### order_payments
- order_id: string (PK part)
- payment_sequential: integer (PK part)
- payment_type: string
- payment_installments: integer
- payment_value: double
- _ingestion_timestamp: timestamp

## Gold Tables

### revenue_daily
- order_purchase_date: date
- total_orders: long
- total_revenue: double
- total_freight: double
- avg_order_value: double
- total_items: long
- unique_customers: long
- unique_sellers: long

### customer_rfm
- customer_unique_id: string
- recency_days: integer
- frequency: long
- monetary: double
- r_score: integer
- f_score: integer
- m_score: integer
- rfm_segment: string
- _computed_at: timestamp

### seller_performance
- seller_id: string
- seller_city: string
- seller_state: string
- total_orders: long
- total_revenue: double
- total_products_sold: long
- avg_review_score: double
- avg_delivery_delay_days: double
- on_time_delivery_rate: double
- _computed_at: timestamp
