# Copilot Workspace Instructions

This repository is a production-style PySpark data engineering project.

## Principles

- Preserve medallion architecture boundaries (Bronze/Silver/Gold).
- Keep jobs idempotent and safe for reruns.
- Prefer schema enforcement in Silver and analytics-only logic in Gold.
- Keep config-driven behavior in `configs/`.
- Add or update tests in `tests/` with any behavior change.

## Coding Rules

- Use typed, deterministic PySpark transforms.
- Avoid Python UDFs when native Spark SQL functions exist.
- Keep logging structured and concise.
- Avoid hardcoded environment-specific absolute paths.

## Project Conventions

- Entry points live in `jobs/`.
- Transformation logic lives in `src/transformations/`.
- Data quality checks live in `src/quality/`.
- Shared schemas and business rules live in `src/models/`.
