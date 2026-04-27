"""Smoke tests for SCD Type 2 helper behavior contracts."""

from pyspark.sql import functions as F


def test_scd_helper_module_imports():
    from src.transformations.scd_handler import apply_scd_type2
    assert callable(apply_scd_type2)
