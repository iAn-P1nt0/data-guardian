from __future__ import annotations

import re

import pandas as pd
import polars as pl
import pytest
from pydantic import BaseModel, Field

from pandera_unified_validator import (
    DataGuardianValidator,
    SchemaBuilder,
    UnifiedSchema,
    UnifiedValidator,
    ValidationSchema,
)
from pandera_unified_validator.backends import PandasBackend, PolarsBackend


# Email regex pattern for validation
EMAIL_REGEX = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"


@pytest.fixture()
def pandas_validator() -> DataGuardianValidator:
    validator = DataGuardianValidator()
    validator.register_backend(PandasBackend())
    return validator


@pytest.fixture()
def sample_dataframe() -> pd.DataFrame:
    """Sample pandas DataFrame for testing."""
    return pd.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "email": ["test@example.com", "invalid", "user@test.com", None, "admin@example.com"],
        "age": [25, 150, 30, -5, 45],
        "score": [85.5, 92.3, 78.1, 88.9, 95.0],
        "active": [True, False, True, True, False],
        "created_at": pd.to_datetime([
            "2024-01-01",
            "2024-02-01",
            "2024-03-01",
            "2024-04-01",
            "2024-05-01"
        ])
    })


@pytest.fixture()
def sample_polars_dataframe() -> pl.DataFrame:
    """Sample polars DataFrame for testing."""
    return pl.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "email": ["test@example.com", "invalid", "user@test.com", None, "admin@example.com"],
        "age": [25, 150, 30, -5, 45],
        "score": [85.5, 92.3, 78.1, 88.9, 95.0],
        "active": [True, False, True, True, False],
        "created_at": [
            "2024-01-01",
            "2024-02-01",
            "2024-03-01",
            "2024-04-01",
            "2024-05-01"
        ]
    })


@pytest.fixture()
def sample_schema() -> UnifiedSchema:
    """Sample validation schema for testing."""
    return (
        SchemaBuilder("test_schema")
        .add_column("id", int, nullable=False, unique=True, ge=0)
        .add_column("email", str, nullable=False, pattern=EMAIL_REGEX)
        .add_column("age", int, nullable=False, ge=0, le=120)
        .add_column("score", float, nullable=False, ge=0.0, le=100.0)
        .add_column("active", bool, nullable=False)
        .add_column("created_at", "datetime64[ns]", nullable=False)
        .build()
    )


@pytest.fixture()
def sample_pydantic_model() -> type[BaseModel]:
    """Sample Pydantic model for testing."""
    class UserRecord(BaseModel):
        id: int = Field(ge=0)
        email: str = Field(pattern=EMAIL_REGEX)
        age: int = Field(ge=0, le=120)
        score: float = Field(ge=0.0, le=100.0)
        active: bool

    return UserRecord


@pytest.fixture()
def valid_sample_dataframe() -> pd.DataFrame:
    """Sample pandas DataFrame that passes validation."""
    return pd.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "email": [
            "test@example.com",
            "user@test.com",
            "admin@example.com",
            "john@domain.org",
            "jane@company.net"
        ],
        "age": [25, 30, 45, 28, 35],
        "score": [85.5, 92.3, 78.1, 88.9, 95.0],
        "active": [True, False, True, True, False],
        "created_at": pd.to_datetime([
            "2024-01-01",
            "2024-02-01",
            "2024-03-01",
            "2024-04-01",
            "2024-05-01"
        ])
    })


@pytest.fixture()
def invalid_sample_dataframe() -> pd.DataFrame:
    """Sample pandas DataFrame with multiple validation errors."""
    return pd.DataFrame({
        "id": [-1, 2, 2, 4, 5000],  # Negative, duplicate
        "email": ["invalid", "no-at-sign", "test@example.com", "bad@", "x@y.z"],
        "age": [-5, 150, 30, 200, 45],  # Out of range
        "score": [-10.0, 150.0, 78.1, 88.9, 95.0],  # Out of range
        "active": [True, False, True, True, False],
        "created_at": pd.to_datetime([
            "2024-01-01",
            "2024-02-01",
            "2024-03-01",
            "2024-04-01",
            "2024-05-01"
        ])
    })


@pytest.fixture()
def unified_validator(sample_schema: UnifiedSchema) -> UnifiedValidator:
    """UnifiedValidator instance with sample schema."""
    validation_schema = sample_schema.to_validation_schema()
    return UnifiedValidator(validation_schema, lazy=True)
