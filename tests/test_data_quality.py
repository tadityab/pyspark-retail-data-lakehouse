"""Tests for data quality checks."""

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from src.quality.data_quality_checks import DataQualityChecker


class TestNotNullCheck:
    """Tests for null value detection."""

    def test_passes_when_no_nulls(self, spark, sample_orders):
        checker = DataQualityChecker()
        result = checker.check_not_null(sample_orders, "order_id", "test_not_null")
        assert result.passed is True

    def test_fails_when_nulls_present(self, spark):
        data = [("ord_001",), (None,), ("ord_003",)]
        df = spark.createDataFrame(data, ["order_id"])
        checker = DataQualityChecker()
        result = checker.check_not_null(df, "order_id", "test_not_null")
        assert result.passed is False
        assert "null_count=1" in result.details


class TestUniqueCheck:
    """Tests for uniqueness detection."""

    def test_fails_with_duplicates(self, spark, sample_orders):
        # sample_orders has a duplicate ord_001
        checker = DataQualityChecker()
        result = checker.check_unique(sample_orders, "order_id", "test_unique")
        assert result.passed is False

    def test_passes_when_unique(self, spark, sample_customers):
        checker = DataQualityChecker()
        result = checker.check_unique(sample_customers, "customer_id", "test_unique")
        assert result.passed is True


class TestAcceptedValuesCheck:
    """Tests for accepted values validation."""

    def test_passes_with_valid_values(self, spark, sample_orders):
        checker = DataQualityChecker()
        valid_statuses = ["delivered", "shipped", "canceled", "processing"]
        result = checker.check_accepted_values(
            sample_orders, "order_status", valid_statuses, "test_values"
        )
        assert result.passed is True

    def test_fails_with_invalid_values(self, spark):
        data = [("delivered",), ("INVALID_STATUS",)]
        df = spark.createDataFrame(data, ["order_status"])
        checker = DataQualityChecker()
        result = checker.check_accepted_values(
            df, "order_status", ["delivered", "shipped"], "test_values"
        )
        assert result.passed is False


class TestReferentialIntegrity:
    """Tests for foreign key validation."""

    def test_passes_when_all_keys_exist(self, spark, sample_order_items, sample_orders):
        checker = DataQualityChecker()
        result = checker.check_referential_integrity(
            sample_order_items, sample_orders, "order_id", "order_id", "test_fk"
        )
        assert result.passed is True

    def test_fails_with_orphan_records(self, spark):
        parent = spark.createDataFrame([("ord_001",)], ["order_id"])
        child = spark.createDataFrame([("ord_001",), ("ord_999",)], ["order_id"])
        checker = DataQualityChecker()
        result = checker.check_referential_integrity(
            child, parent, "order_id", "order_id", "test_fk"
        )
        assert result.passed is False
        assert "orphan_records=1" in result.details


class TestSummary:
    """Tests for the summary method."""

    def test_summary_counts(self, spark, sample_orders):
        checker = DataQualityChecker()
        checker.check_not_null(sample_orders, "order_id", "check1", "critical")
        checker.check_unique(sample_orders, "order_id", "check2", "critical")

        summary = checker.summary()
        assert summary["total_checks"] == 2
        assert summary["passed"] + summary["failed"] == 2

    def test_critical_failures_detected(self, spark):
        from pyspark.sql.types import StructType, StructField, StringType
        schema = StructType([StructField("order_id", StringType(), True)])
        data = [(None,)]
        df = spark.createDataFrame(data, schema)
        checker = DataQualityChecker()
        checker.check_not_null(df, "order_id", "check1", "critical")
        assert checker.has_critical_failures() is True
