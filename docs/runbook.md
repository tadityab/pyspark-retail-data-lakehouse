# Runbook

## Prerequisites

- Python 3.11+
- Java 17+
- Spark-compatible environment

## Local Setup

1. Install dependencies:
   - `pip install -e .`
2. Put source files in `data/raw/olist`
3. Run tests:
   - `pytest -q`

## Pipeline Execution

- Bronze:
  - `python jobs/run_bronze.py --env local`
- Silver:
  - `python jobs/run_silver.py --env local`
- Gold:
  - `python jobs/run_gold.py --env local`
- Full run:
  - `python jobs/run_full_pipeline.py --env local`

## Backfill

- For a full rebuild, clear `data/lakehouse` and rerun full pipeline.
- For incremental reruns, keep `_checkpoints` for watermark continuity.

## Monitoring

Track these indicators each run:
- Runtime by stage
- Input vs output row counts per table
- Data quality failures and warning counts
- Partition counts and output file volume

## Common Failures

### Missing source file
- Symptom: file-not-found during Bronze
- Action: verify raw file names in `data/raw/olist`

### Schema parse issues
- Symptom: null-heavy columns in Silver
- Action: inspect source formatting and update schema/casting logic

### DQ critical failure
- Symptom: Silver job exits with critical DQ status
- Action: inspect rejected records under `data/lakehouse/_rejected`

### Slow joins
- Symptom: long shuffle stages
- Action: verify broadcast thresholds and partition strategy
