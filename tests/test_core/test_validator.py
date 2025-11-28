from __future__ import annotations

import pandas as pd
from pandera import Column, Check, DataFrameSchema
from pydantic import BaseModel
import pytest

from pandera_unified_validator import DataGuardianValidator, ValidationSchema
from pandera_unified_validator.backends import PandasBackend


class Order(BaseModel):
    order_id: int
    amount: float


def build_schema() -> ValidationSchema:
    return ValidationSchema(
        name="orders",
        record_model=Order,
        dataframe_schema=DataFrameSchema(
            {
                "order_id": Column(int, Check.greater_than_or_equal_to(0)),
                "amount": Column(float, Check.greater_than_or_equal_to(0)),
            }
        ),
    )


def test_pandas_backend_success(pandas_validator: DataGuardianValidator) -> None:
    df = pd.DataFrame(
        [
            {"order_id": 1, "amount": 10.0},
            {"order_id": 2, "amount": 5.5},
        ]
    )

    report = pandas_validator.validate(df, build_schema())

    assert report.is_valid
    assert report.metadata.get("backend") == "pandas"


def test_validator_requires_backend_when_unavailable() -> None:
    validator = DataGuardianValidator()
    schema = build_schema()

    with pytest.raises(ValueError):
        validator.validate(pd.DataFrame(), schema)

    # Registering makes the validation succeed
    validator.register_backend(PandasBackend())
    empty_df = pd.DataFrame({"order_id": pd.array([], dtype="int64"), "amount": pd.array([], dtype="float64")})
    empty_report = validator.validate(empty_df, schema)
    assert empty_report.is_valid
