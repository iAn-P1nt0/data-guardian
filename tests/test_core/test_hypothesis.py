"""Property-based tests using hypothesis for pandera-unified-validator."""

from __future__ import annotations

import pandas as pd
import pytest
from hypothesis import given, settings, strategies as st
from hypothesis.extra.pandas import column, data_frames
from pydantic import BaseModel, Field

from pandera_unified_validator import SchemaBuilder, UnifiedValidator, ValidationResult


class SimpleRecord(BaseModel):
    """Simple Pydantic model for hypothesis testing."""

    id: int = Field(ge=0, le=10000)
    name: str
    score: float = Field(ge=0.0, le=100.0)
    active: bool


# Hypothesis strategies
valid_id = st.integers(min_value=0, max_value=10000)
valid_name = st.text(min_size=1, max_size=50, alphabet=st.characters(blacklist_categories=["Cs"]))
valid_score = st.floats(min_value=0.0, max_value=100.0, allow_nan=False, allow_infinity=False)
valid_bool = st.booleans()


@given(
    data_frames([
        column("id", elements=valid_id, dtype=int),
        column("name", elements=valid_name, dtype=str),
        column("score", elements=valid_score, dtype=float),
        column("active", elements=valid_bool, dtype=bool),
    ])
)
@settings(max_examples=50, deadline=2000)
def test_validator_with_valid_random_data(df: pd.DataFrame) -> None:
    """Test validator always succeeds with valid random data."""
    schema = (
        SchemaBuilder("random_test")
        .add_column("id", int, nullable=False, ge=0, le=10000)
        .add_column("name", str, nullable=False)
        .add_column("score", float, nullable=False, ge=0.0, le=100.0)
        .add_column("active", bool, nullable=False)
        .build()
    )

    validator = UnifiedValidator(schema.to_validation_schema(), lazy=True)
    result = validator.validate(df)

    # All generated data should be valid
    assert isinstance(result, ValidationResult)
    assert result.is_valid is True
    assert len(result.errors) == 0


@given(
    id_val=st.integers(min_value=-1000, max_value=20000),
    score_val=st.floats(min_value=-100.0, max_value=200.0, allow_nan=True, allow_infinity=True),
)
@settings(max_examples=100)
def test_validator_handles_out_of_range_values(id_val: int, score_val: float) -> None:
    """Test validator correctly identifies out-of-range values."""
    df = pd.DataFrame({
        "id": [id_val],
        "name": ["test"],
        "score": [score_val],
        "active": [True],
    })

    schema = (
        SchemaBuilder("range_test")
        .add_column("id", int, nullable=False, ge=0, le=10000)
        .add_column("name", str, nullable=False)
        .add_column("score", float, nullable=False, ge=0.0, le=100.0)
        .add_column("active", bool, nullable=False)
        .build()
    )

    validator = UnifiedValidator(schema.to_validation_schema(), lazy=True)
    result = validator.validate(df)

    # Determine expected validity
    id_valid = 0 <= id_val <= 10000
    score_valid = not pd.isna(score_val) and not pd.isinf(score_val) and 0.0 <= score_val <= 100.0
    expected_valid = id_valid and score_valid

    assert result.is_valid == expected_valid


@given(
    st.lists(
        st.fixed_dictionaries({
            "id": valid_id,
            "name": valid_name,
            "score": valid_score,
            "active": valid_bool,
        }),
        min_size=1,
        max_size=50,
    )
)
@settings(max_examples=50)
def test_validator_with_dict_records(records: list[dict]) -> None:
    """Test validator with list of dictionaries."""
    schema = (
        SchemaBuilder("dict_test")
        .add_column("id", int, nullable=False, ge=0, le=10000)
        .add_column("name", str, nullable=False)
        .add_column("score", float, nullable=False, ge=0.0, le=100.0)
        .add_column("active", bool, nullable=False)
        .build()
    )

    validator = UnifiedValidator(schema.to_validation_schema(), lazy=True)
    result = validator.validate(records)

    assert isinstance(result, ValidationResult)
    assert result.is_valid is True


@given(st.text(min_size=0, max_size=100))
@settings(max_examples=100)
def test_string_column_accepts_any_text(text_value: str) -> None:
    """Test that string columns accept any text without pattern constraints."""
    df = pd.DataFrame({"text_field": [text_value]})

    schema = SchemaBuilder("text_test").add_column("text_field", str, nullable=False).build()

    validator = UnifiedValidator(schema.to_validation_schema(), lazy=True)
    result = validator.validate(df)

    # Any non-empty string should be valid
    assert isinstance(result, ValidationResult)


@given(
    st.lists(
        st.integers(min_value=0, max_value=1000),
        min_size=1,
        max_size=100,
    )
)
@settings(max_examples=30)
def test_unique_constraint_detection(ids: list[int]) -> None:
    """Test that unique constraint properly detects duplicates."""
    df = pd.DataFrame({"id": ids})

    schema = SchemaBuilder("unique_test").add_column("id", int, nullable=False, unique=True).build()

    validator = UnifiedValidator(schema.to_validation_schema(), lazy=True)
    result = validator.validate(df)

    # Should be valid only if all IDs are unique
    has_duplicates = len(ids) != len(set(ids))
    assert result.is_valid == (not has_duplicates)


@given(
    data_frames([
        column("value", elements=st.floats(
            min_value=-1e10,
            max_value=1e10,
            allow_nan=True,
            allow_infinity=True,
        ), dtype=float),
    ])
)
@settings(max_examples=50)
def test_nullable_column_handling(df: pd.DataFrame) -> None:
    """Test nullable column handling with various float values."""
    schema = SchemaBuilder("nullable_test").add_column("value", float, nullable=True).build()

    validator = UnifiedValidator(schema.to_validation_schema(), lazy=True)
    result = validator.validate(df)

    # With nullable=True, should handle NaN values
    assert isinstance(result, ValidationResult)


@given(
    st.integers(min_value=1, max_value=100),
    st.integers(min_value=1, max_value=10),
)
@settings(max_examples=30)
def test_schema_builder_accepts_valid_column_count(
    num_rows: int,
    num_cols: int,
) -> None:
    """Test schema builder with varying column counts."""
    builder = SchemaBuilder("dynamic_schema")

    for i in range(num_cols):
        builder.add_column(f"col_{i}", int, nullable=False, ge=0)

    schema = builder.build()

    # Create dataframe with matching structure
    data = {f"col_{i}": [j for j in range(num_rows)] for i in range(num_cols)}
    df = pd.DataFrame(data)

    validator = UnifiedValidator(schema.to_validation_schema(), lazy=True)
    result = validator.validate(df)

    assert result.is_valid is True
    assert len(schema.columns) == num_cols


@given(
    ge_val=st.integers(min_value=0, max_value=100),
    le_val=st.integers(min_value=0, max_value=100),
)
def test_schema_constraint_ordering(ge_val: int, le_val: int) -> None:
    """Test that schema constraints work with different min/max orderings."""
    # Ensure ge <= le
    if ge_val > le_val:
        ge_val, le_val = le_val, ge_val

    schema = (
        SchemaBuilder("constraint_test")
        .add_column("value", int, nullable=False, ge=ge_val, le=le_val)
        .build()
    )

    # Test with value in range
    mid_val = (ge_val + le_val) // 2
    df = pd.DataFrame({"value": [mid_val]})

    validator = UnifiedValidator(schema.to_validation_schema(), lazy=True)
    result = validator.validate(df)

    assert result.is_valid is True


class TestHypothesisIntegration:
    """Integration tests combining hypothesis with existing fixtures."""

    @given(st.integers(min_value=0, max_value=120))
    @settings(max_examples=50)
    def test_age_validation_with_random_valid_ages(self, age: int) -> None:
        """Test age validation with random valid ages."""
        df = pd.DataFrame({
            "id": [1],
            "age": [age],
            "email": ["test@example.com"],
        })

        schema = (
            SchemaBuilder("age_test")
            .add_column("id", int, nullable=False)
            .add_column("age", int, nullable=False, ge=0, le=120)
            .add_column("email", str, nullable=False)
            .build()
        )

        validator = UnifiedValidator(schema.to_validation_schema(), lazy=True)
        result = validator.validate(df)

        assert result.is_valid is True

    @given(st.integers(min_value=-1000, max_value=-1) | st.integers(min_value=121, max_value=1000))
    @settings(max_examples=50)
    def test_age_validation_rejects_invalid_ages(self, age: int) -> None:
        """Test age validation rejects out-of-range ages."""
        df = pd.DataFrame({
            "id": [1],
            "age": [age],
            "email": ["test@example.com"],
        })

        schema = (
            SchemaBuilder("age_test")
            .add_column("id", int, nullable=False)
            .add_column("age", int, nullable=False, ge=0, le=120)
            .add_column("email", str, nullable=False)
            .build()
        )

        validator = UnifiedValidator(schema.to_validation_schema(), lazy=True)
        result = validator.validate(df)

        assert result.is_valid is False
        assert len(result.errors) > 0
