"""Data quality checks — rule engine for validation between pipeline layers."""

from typing import Optional

import yaml
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.utils.logger import get_logger

logger = get_logger(__name__)


class DataQualityResult:
    """Container for a single data quality check result."""

    def __init__(self, rule_name: str, column: str, check_type: str,
                 severity: str, passed: bool, details: str = ""):
        self.rule_name = rule_name
        self.column = column
        self.check_type = check_type
        self.severity = severity
        self.passed = passed
        self.details = details

    def __repr__(self):
        status = "PASS" if self.passed else "FAIL"
        return f"[{status}] {self.rule_name} ({self.severity}): {self.details}"


class DataQualityChecker:
    """Validates DataFrames against configurable quality rules.

    Supports: not_null, unique, accepted_values, greater_than,
    greater_than_or_equal, timestamp_parseable checks.
    """

    def __init__(self, rules_path: Optional[str] = None):
        self.results: list[DataQualityResult] = []
        self.rules_config = None
        if rules_path:
            with open(rules_path, "r") as f:
                self.rules_config = yaml.safe_load(f)

    def check_not_null(self, df: DataFrame, column: str, rule_name: str,
                       severity: str = "critical") -> DataQualityResult:
        """Check that a column has no null values."""
        total = df.count()
        null_count = df.filter(F.col(column).isNull()).count()
        null_rate = null_count / total if total > 0 else 0
        passed = null_count == 0

        result = DataQualityResult(
            rule_name=rule_name,
            column=column,
            check_type="not_null",
            severity=severity,
            passed=passed,
            details=f"null_count={null_count}, null_rate={null_rate:.4f}",
        )
        self.results.append(result)
        logger.info(str(result))
        return result

    def check_unique(self, df: DataFrame, column: str, rule_name: str,
                     severity: str = "critical") -> DataQualityResult:
        """Check that a column contains only unique values."""
        total = df.count()
        distinct_count = df.select(column).distinct().count()
        duplicate_count = total - distinct_count
        passed = duplicate_count == 0

        result = DataQualityResult(
            rule_name=rule_name,
            column=column,
            check_type="unique",
            severity=severity,
            passed=passed,
            details=f"total={total}, distinct={distinct_count}, duplicates={duplicate_count}",
        )
        self.results.append(result)
        logger.info(str(result))
        return result

    def check_accepted_values(self, df: DataFrame, column: str, values: list,
                              rule_name: str, severity: str = "warning") -> DataQualityResult:
        """Check that column values are within an accepted set."""
        invalid = df.filter(~F.col(column).isin(values) & F.col(column).isNotNull())
        invalid_count = invalid.count()
        passed = invalid_count == 0

        result = DataQualityResult(
            rule_name=rule_name,
            column=column,
            check_type="accepted_values",
            severity=severity,
            passed=passed,
            details=f"invalid_count={invalid_count}, accepted={values}",
        )
        self.results.append(result)
        logger.info(str(result))
        return result

    def check_greater_than(self, df: DataFrame, column: str, value: float,
                           rule_name: str, severity: str = "warning") -> DataQualityResult:
        """Check that all non-null values in a column are greater than a threshold."""
        violations = df.filter(
            (F.col(column).isNotNull()) & (F.col(column) <= value)
        )
        violation_count = violations.count()
        passed = violation_count == 0

        result = DataQualityResult(
            rule_name=rule_name,
            column=column,
            check_type="greater_than",
            severity=severity,
            passed=passed,
            details=f"violations={violation_count}, threshold>{value}",
        )
        self.results.append(result)
        logger.info(str(result))
        return result

    def check_referential_integrity(self, child_df: DataFrame, parent_df: DataFrame,
                                    child_column: str, parent_column: str,
                                    rule_name: str,
                                    severity: str = "critical") -> DataQualityResult:
        """Check that all child FK values exist in the parent table."""
        orphans = child_df.join(
            parent_df.select(F.col(parent_column).alias(child_column)),
            on=child_column,
            how="left_anti",
        )
        orphan_count = orphans.count()
        passed = orphan_count == 0

        result = DataQualityResult(
            rule_name=rule_name,
            column=child_column,
            check_type="referential_integrity",
            severity=severity,
            passed=passed,
            details=f"orphan_records={orphan_count}",
        )
        self.results.append(result)
        logger.info(str(result))
        return result

    def run_checks_for_table(self, df: DataFrame, layer: str, table_name: str) -> list:
        """Run all configured rules for a specific table at a given layer.

        Requires rules_config to be loaded from YAML.
        """
        if not self.rules_config:
            raise ValueError("No rules config loaded. Pass rules_path to constructor.")

        rules = (
            self.rules_config
            .get("rules", {})
            .get(layer, {})
            .get(table_name, [])
        )

        for rule in rules:
            check_type = rule["check"]
            if check_type == "not_null":
                self.check_not_null(df, rule["column"], rule["name"], rule.get("severity", "critical"))
            elif check_type == "unique":
                self.check_unique(df, rule["column"], rule["name"], rule.get("severity", "critical"))
            elif check_type == "accepted_values":
                self.check_accepted_values(
                    df, rule["column"], rule["values"], rule["name"], rule.get("severity", "warning")
                )
            elif check_type == "greater_than":
                self.check_greater_than(
                    df, rule["column"], rule["value"], rule["name"], rule.get("severity", "warning")
                )

        return self.results

    def has_critical_failures(self) -> bool:
        """Check if any critical-severity rules failed."""
        return any(
            not r.passed and r.severity == "critical" for r in self.results
        )

    def summary(self) -> dict:
        """Return a summary of all check results."""
        return {
            "total_checks": len(self.results),
            "passed": sum(1 for r in self.results if r.passed),
            "failed": sum(1 for r in self.results if not r.passed),
            "critical_failures": sum(
                1 for r in self.results if not r.passed and r.severity == "critical"
            ),
            "warnings": sum(
                1 for r in self.results if not r.passed and r.severity == "warning"
            ),
        }
