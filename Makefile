PYTHON ?= python

.PHONY: setup test run bronze silver gold lint clean

setup:
	$(PYTHON) -m pip install -e .[dev]

test:
	$(PYTHON) -m pytest

run:
	$(PYTHON) jobs/run_full_pipeline.py --env local

bronze:
	$(PYTHON) jobs/run_bronze.py --env local

silver:
	$(PYTHON) jobs/run_silver.py --env local

gold:
	$(PYTHON) jobs/run_gold.py --env local

lint:
	$(PYTHON) -m py_compile src/utils/*.py src/models/*.py src/ingestion/*.py src/transformations/*.py src/quality/*.py jobs/*.py

clean:
	@if exist data\lakehouse rmdir /s /q data\lakehouse
	@if exist .pytest_cache rmdir /s /q .pytest_cache
