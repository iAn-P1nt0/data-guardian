from __future__ import annotations

import pandas as pd
import pytest
from pydantic import BaseModel

from data_guardian import SchemaBuilder, SchemaConverter, UnifiedSchema, UnifiedValidator


class TestSchemaBuilder:
    def test_add_column_basic(self) -> None:
        schema = (
            SchemaBuilder("orders")
            .add_column("id", int, unique=True, ge=0)
            .add_column("amount", float, ge=0, le=10000)
            .add_column("status", str, isin=["pending", "completed", "cancelled"])
            .build()
        )

        assert schema.name == "orders"
        assert len(schema.columns) == 3
        assert schema.columns["id"].unique is True
        assert schema.columns["amount"].ge == 0
        assert schema.columns["status"].isin == ["pending", "completed", "cancelled"]

    def test_add_column_with_pattern(self) -> None:
        email_pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
        schema = (
            SchemaBuilder("users")
            .add_column("email", str, pattern=email_pattern)
            .build()
        )

        assert schema.columns["email"].pattern == email_pattern

    def test_add_custom_validator(self) -> None:
        def positive_values(series: pd.Series) -> pd.Series:
            return series > 0

        schema = (
            SchemaBuilder("data")
            .add_column("value", float)
            .add_custom_validator("check_positive", positive_values)
            .build()
        )

        assert len(schema.custom_validators) == 1
        assert schema.custom_validators[0].name == "check_positive"

    def test_add_cross_column_constraint(self) -> None:
        def end_after_start(df: pd.DataFrame) -> pd.Series:
            return df["end_date"] > df["start_date"]

        schema = (
            SchemaBuilder("events")
            .add_column("start_date", "datetime64[ns]")
            .add_column("end_date", "datetime64[ns]")
            .add_cross_column_constraint(
                "end_after_start",
                ["start_date", "end_date"],
                end_after_start,
            )
            .build()
        )

        assert len(schema.cross_column_constraints) == 1
        assert schema.cross_column_constraints[0].name == "end_after_start"

    def test_with_metadata(self) -> None:
        schema = (
            SchemaBuilder("test")
            .add_column("x", int)
            .with_metadata(version="1.0", author="test")
            .build()
        )

        assert schema.metadata["version"] == "1.0"
        assert schema.metadata["author"] == "test"

    def test_to_pandera(self) -> None:
        schema = (
            SchemaBuilder("orders")
            .add_column("id", int, unique=True, ge=0)
            .add_column("amount", float, ge=0)
            .build()
        )

        pandera_schema = schema.to_pandera()
        assert "id" in pandera_schema.columns
        assert "amount" in pandera_schema.columns

    def test_to_pydantic(self) -> None:
        schema = (
            SchemaBuilder("orders")
            .add_column("id", int, nullable=False)
            .add_column("name", str, nullable=True)
            .build()
        )

        model = schema.to_pydantic()
        assert "id" in model.model_fields
        assert "name" in model.model_fields

    def test_to_validation_schema(self) -> None:
        unified = (
            SchemaBuilder("test")
            .add_column("value", int, ge=0)
            .build()
        )

        validation_schema = unified.to_validation_schema()
        assert validation_schema.name == "test"
        assert validation_schema.record_model is not None
        assert validation_schema.dataframe_schema is not None


class TestSchemaConverter:
    def test_from_pydantic(self) -> None:
        class Order(BaseModel):
            id: int
            name: str
            amount: float | None = None

        schema = SchemaConverter.from_pydantic(Order)

        assert schema.name == "Order"
        assert "id" in schema.columns
        assert "name" in schema.columns
        assert "amount" in schema.columns
        assert schema.columns["amount"].nullable is True

    def test_infer_from_dataframe(self) -> None:
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["a", "b", "c"],
            "value": [1.0, 2.0, 3.0],
        })

        schema = SchemaConverter.infer_from_dataframe(df, "inferred")

        assert schema.name == "inferred"
        assert schema.columns["id"].dtype == int
        assert schema.columns["name"].dtype == str
        assert schema.columns["value"].dtype == float

    def test_infer_with_constraints(self) -> None:
        df = pd.DataFrame({
            "score": [10, 20, 30, 40, 50],
            "category": ["A", "A", "B", "B", "A"],
        })

        schema = SchemaConverter.infer_from_dataframe(
            df,
            "constrained",
            infer_constraints=True,
        )

        assert schema.columns["score"].ge == 10.0
        assert schema.columns["score"].le == 50.0
        assert set(schema.columns["category"].isin or []) == {"A", "B"}


class TestUnifiedSchemaSerialization:
    def test_to_json_and_from_json(self) -> None:
        original = (
            SchemaBuilder("test")
            .add_column("id", int, unique=True, ge=0)
            .add_column("name", str, pattern=r"^[A-Z]")
            .with_metadata(version="1.0")
            .build()
        )

        json_str = original.to_json()
        restored = UnifiedSchema.from_json(json_str)

        assert restored.name == original.name
        assert len(restored.columns) == len(original.columns)
        assert restored.columns["id"].unique is True
        assert restored.columns["name"].pattern == r"^[A-Z]"

    def test_to_dict(self) -> None:
        schema = (
            SchemaBuilder("orders")
            .add_column("id", int, ge=0)
            .build()
        )

        data = schema.to_dict()
        assert data["name"] == "orders"
        assert "id" in data["columns"]


class TestIntegrationWithUnifiedValidator:
    def test_schema_builder_with_unified_validator(self) -> None:
        schema = (
            SchemaBuilder("users")
            .add_column("id", int, ge=1)
            .add_column("name", str, nullable=False)
            .build()
        )

        validation_schema = schema.to_validation_schema()
        validator = UnifiedValidator(validation_schema, lazy=True)

        df = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})
        result = validator.validate(df)

        assert result.is_valid
