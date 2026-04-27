"""Tests for schema enforcement and the schema registry."""

import pytest
from pyspark.sql.types import StructType, StringType, TimestampType, IntegerType

from src.models.schemas import get_schema, SCHEMA_REGISTRY


class TestSchemaRegistry:
    """Tests for schema lookup."""

    def test_bronze_orders_exists(self):
        schema = get_schema("bronze", "orders")
        assert isinstance(schema, StructType)
        field_names = [f.name for f in schema.fields]
        assert "order_id" in field_names

    def test_silver_orders_has_stronger_types(self):
        schema = get_schema("silver", "orders")
        type_map = {f.name: f.dataType for f in schema.fields}
        assert isinstance(type_map["order_purchase_timestamp"], TimestampType)
        assert type_map["order_id"].simpleString() == "string"

    def test_gold_schemas_exist(self):
        for table in ["revenue_daily", "customer_rfm", "seller_performance"]:
            schema = get_schema("gold", table)
            assert isinstance(schema, StructType)

    def test_unknown_layer_raises(self):
        with pytest.raises(ValueError, match="Unknown layer"):
            get_schema("platinum", "orders")

    def test_unknown_table_raises(self):
        with pytest.raises(ValueError, match="Unknown table"):
            get_schema("bronze", "nonexistent_table")

    def test_bronze_schemas_all_strings(self):
        """Bronze layer should accept all data as strings (schema-on-read)."""
        for table_name, schema in SCHEMA_REGISTRY["bronze"].items():
            for field in schema.fields:
                assert isinstance(field.dataType, StringType), (
                    f"Bronze {table_name}.{field.name} should be StringType, "
                    f"got {field.dataType}"
                )

    def test_silver_orders_non_nullable_keys(self):
        schema = get_schema("silver", "orders")
        order_id_field = next(f for f in schema.fields if f.name == "order_id")
        assert order_id_field.nullable is False
