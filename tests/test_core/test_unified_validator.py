from __future__ import annotations

import pandas as pd
import polars as pl
import pytest
from pydantic import BaseModel, Field

from pandera_unified_validator import UnifiedValidator
from pandera_unified_validator.core.validator import ValidationFailedError


class Person(BaseModel):
    identifier: int
    name: str = Field(min_length=1)


def test_unified_validator_accepts_dicts() -> None:
    validator = UnifiedValidator(Person)

    result = validator.validate(
        [
            {"identifier": 1, "name": "Ada"},
            {"identifier": 2, "name": "Alan"},
        ]
    )

    assert result.is_valid
    assert result.metadata.get("backend") == "pandas"


def test_unified_validator_detects_errors_eager() -> None:
    validator = UnifiedValidator(Person, lazy=False)

    with pytest.raises(ValidationFailedError):
        validator.validate({"identifier": "oops", "name": ""})


def test_unified_validator_lazy_collects_errors() -> None:
    validator = UnifiedValidator(Person, lazy=True)

    result = validator.validate({"identifier": "oops", "name": ""})

    assert not result.is_valid
    assert result.errors


def test_unified_validator_handles_polars_backend() -> None:
    frame = pl.DataFrame({"identifier": [1], "name": ["Ada"]})
    validator = UnifiedValidator(Person, backend=None)

    result = validator.validate(frame)

    assert result.is_valid
    assert result.metadata.get("backend") == "polars"


def test_unified_validator_streaming_chunks() -> None:
    validator = UnifiedValidator(Person)
    data_stream = iter({"identifier": idx, "name": f"Person {idx}"} for idx in range(3))

    results = list(validator.validate_streaming(data_stream, chunk_size=2))

    assert len(results) == 2
    assert all(result.is_valid for result in results)


def test_unified_validator_auto_fix_suggestions() -> None:
    validator = UnifiedValidator(Person, auto_fix=True, lazy=True)
    df = pd.DataFrame({"identifier": [1, 2]})

    result = validator.validate(df)

    assert result.suggestions
    fixed = validator.apply_fixes(df, result)
    assert "name" in fixed.columns
