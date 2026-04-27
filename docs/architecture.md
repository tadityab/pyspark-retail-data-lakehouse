# Architecture

## High-Level Data Flow

```
  Raw CSVs (Olist / UK Retail / Synthetic)
         │
         ▼
  ┌─────────────────┐
  │   BRONZE LAYER   │   Schema-on-read · Append-only · Parquet
  │   Raw Ingestion  │   Partitioned by _ingestion_date
  └────────┬─────────┘
           │
           ▼
  ┌─────────────────┐   ┌──────────────────┐
  │   SILVER LAYER   │──▶│  DQ Reports      │
  │   Cleansed       │   │  Quarantine      │
  │   Conformed      │   └──────────────────┘
  │   SCD Type 2     │
  │   Schema-on-write│
  └────────┬─────────┘
           │
           ▼
  ┌─────────────────┐   ┌──────────────────┐
  │    GOLD LAYER    │──▶│  Analytics / BI  │
  │   Business KPIs  │   │  Revenue KPIs    │
  │   RFM Segments   │   │  Customer Segs   │
  │   Seller Scores  │   │  Seller Cards    │
  └──────────────────┘   └──────────────────┘
```

## Design Decisions

### Why Medallion Architecture?
- **Bronze**: Never lose raw data. Append-only = audit trail. Schema-on-read accepts anything.
- **Silver**: Single source of truth. Schema enforcement catches drift. SCD2 preserves history.
- **Gold**: Pre-aggregated for fast queries. BI dashboards hit Gold, never Silver.

### Why Parquet + Delta?
- **Parquet** (Bronze/Silver): Columnar, compressed (10x smaller than CSV), predicate pushdown.
- **Delta** (Gold): ACID transactions, time travel, `MERGE` for upserts.

### Why Incremental Processing?
Full refresh at 50M rows: ~45 minutes, high compute cost.
Incremental (watermark-based): ~45 seconds for daily delta.
85% cost reduction, same output.

### Why This Join Strategy?
- **Broadcast**: `products` (32K rows) and `sellers` (3K rows) → eliminates shuffle entirely.
- **Sort-Merge**: `order_items × orders` (both large) → pre-partitioned on `order_id`.
- **Salted joins**: Top 1% of sellers have 40% of orders → salt `seller_id` to distribute evenly.

### Why SCD Type 2?
Customer addresses and seller locations change over time. Without SCD2, you can't answer:
"What city was the customer in when they placed order #4521?"

### Partitioning Strategy
- **Bronze**: `_ingestion_date` — one partition per load, simple append.
- **Silver orders**: `order_purchase_date` — enables partition pruning for date-range queries.
- **Gold**: No partitioning (small aggregated tables) — coalesced to 4 files.

### Idempotency
All writes use `partitionOverwriteMode = dynamic`. Re-running a failed job replaces only affected partitions without duplicating data.
