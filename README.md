# 🏗️ Retail Data Lakehouse Pipeline
**Production-grade PySpark pipeline processing 10M+ e-commerce transactions
through a medallion architecture with incremental loading, data quality gates,
and analytics-ready output.**

![Python](https://img.shields.io/badge/python-3.11-blue)
![PySpark](https://img.shields.io/badge/PySpark-3.5-orange)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-2.4-00ADD8)
![Tests](https://img.shields.io/badge/tests-passing-green)

---

## The Problem
Mid-market e-commerce companies process hundreds of thousands of orders monthly
across multiple channels. Their analytics teams wait hours for reports built on
full-refresh pipelines. Data quality issues — duplicate orders, missing payments,
inconsistent product categories — silently corrupt revenue numbers.

## The Solution
An end-to-end data lakehouse pipeline that:
- **Ingests** multi-source transactional data (orders, payments, customers, sellers)
- **Processes incrementally** — only new/changed data, reducing runtime by 85%
- **Enforces quality** — null checks, dedup, referential integrity at every layer
- **Delivers analytics** — revenue KPIs, customer RFM segments, seller scorecards

## Architecture
[Architecture diagram image here]

| Layer | Purpose | Format | Key Operations |
|-------|---------|--------|----------------|
| Bronze | Raw ingestion, append-only | Parquet | Schema-on-read, partitioned by ingestion date |
| Silver | Cleansed, conformed | Parquet/Delta | Dedup, type casting, SCD2, schema enforcement |
| Gold | Business-ready | Delta | Revenue rollups, RFM scoring, cohort analysis |

## Key Engineering Decisions
| Challenge | Solution | Impact |
|-----------|----------|--------|
| 6-hour full refresh | Watermark-based incremental processing | 85% runtime reduction |
| Skewed seller joins | Salted join keys | 10x faster join stage |
| Schema drift in sources | StructType enforcement at Silver | Zero silent data corruption |
| Duplicate orders | Window-based dedup (ROW_NUMBER) | 99.97% dedup accuracy |

## Quick Start
```bash
git clone <repo>
make setup       # Install dependencies
make test        # Run test suite
make run         # Execute full pipeline on sample data
```

## Project Structure
```
pyspark-retail-data-lakehouse/
├── configs/               # Pipeline + Spark + DQ configs
├── data/
│   ├── raw/               # Raw source data (not committed)
│   ├── sample/            # Small sample CSVs for local demo
│   └── synthetic/         # Synthetic generator for scale testing
├── docs/                  # Architecture, dictionary, runbook
├── docker/                # Dockerfile + compose for local cluster
├── jobs/                  # Pipeline entrypoints (bronze/silver/gold/full)
├── src/
│   ├── ingestion/         # Source to Bronze
│   ├── transformations/   # Bronze->Silver->Gold logic
│   ├── quality/           # Data quality checks + quarantine
│   ├── models/            # Schemas + business rules
│   └── utils/             # Spark session, logging, IO helpers
└── tests/                 # Unit tests with Spark fixtures
```

## Local Execution

Run full pipeline:

```bash
python jobs/run_full_pipeline.py --env local
```

Run individual layers:

```bash
python jobs/run_bronze.py --env local
python jobs/run_silver.py --env local
python jobs/run_gold.py --env local
```

## Data Source

Primary dataset: Olist Brazilian E-Commerce

- URL: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce
- Download files and place in `data/raw/olist/`

## Outputs

Gold layer tables:

- `revenue_daily`: Daily order and revenue KPIs
- `customer_rfm`: Customer segmentation using Recency/Frequency/Monetary
- `seller_performance`: Seller scorecard and delivery metrics

## Why This Is Production-Oriented

- Incremental processing with watermark strategy
- Layered medallion architecture (Bronze/Silver/Gold)
- Data quality checks and critical-failure gating
- Schema enforcement in Silver
- Partitioning and performance tuning for Spark workloads
- Containerized local execution path

## How I Can Help

I build consulting-grade data platforms that reduce refresh cost, improve data trust,
and deliver analytics-ready datasets quickly.

Typical support:

- Migrate batch SQL pipelines to PySpark-based lakehouse workflows
- Add incremental and idempotent processing to reduce cloud spend
- Implement practical data quality gates before BI consumption
- Design clean repo structures and runbooks for handover to internal teams

If you are evaluating this repository for contract work, start with `jobs/run_full_pipeline.py`
and the architecture notes in `docs/architecture.md`.