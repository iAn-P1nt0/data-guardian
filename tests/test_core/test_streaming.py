from __future__ import annotations

import asyncio
import tempfile
from pathlib import Path
from typing import AsyncIterator, Dict, Any, List

import pandas as pd
import pytest

from pandera_unified_validator import (
    SchemaBuilder,
    StreamingResult,
    StreamingValidator,
    ValidationMetrics,
    validate_csv_streaming_sync,
)


@pytest.fixture
def sample_schema():
    return (
        SchemaBuilder("test_schema")
        .add_column("id", int, ge=1)
        .add_column("name", str, nullable=False)
        .add_column("value", float, ge=0)
        .build()
    )


@pytest.fixture
def valid_csv_file(tmp_path: Path) -> Path:
    filepath = tmp_path / "valid.csv"
    df = pd.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
        "value": [10.0, 20.0, 30.0, 40.0, 50.0],
    })
    df.to_csv(filepath, index=False)
    return filepath


@pytest.fixture
def invalid_csv_file(tmp_path: Path) -> Path:
    filepath = tmp_path / "invalid.csv"
    df = pd.DataFrame({
        "id": [1, 2, -3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
        "value": [10.0, -20.0, 30.0, 40.0, 50.0],
    })
    df.to_csv(filepath, index=False)
    return filepath


@pytest.fixture
def high_error_csv_file(tmp_path: Path) -> Path:
    filepath = tmp_path / "high_error.csv"
    df = pd.DataFrame({
        "id": [-1, -2, -3, -4, -5, -6, -7, -8, -9, -10],
        "name": ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J"],
        "value": [-1.0, -2.0, -3.0, -4.0, -5.0, -6.0, -7.0, -8.0, -9.0, -10.0],
    })
    df.to_csv(filepath, index=False)
    return filepath


class TestStreamingValidator:
    def test_validate_valid_csv(self, sample_schema, valid_csv_file: Path) -> None:
        validator = StreamingValidator(sample_schema, chunk_size=2)
        result = validator.validate_csv_sync(valid_csv_file)

        assert result.is_valid
        assert result.metrics.total_rows == 5
        assert result.metrics.invalid_rows == 0
        assert result.metrics.error_rate == 0.0
        assert result.schema_name == "test_schema"

    def test_validate_invalid_csv(self, sample_schema, invalid_csv_file: Path) -> None:
        validator = StreamingValidator(sample_schema, chunk_size=2)
        result = validator.validate_csv_sync(invalid_csv_file)

        assert not result.is_valid
        assert result.metrics.total_rows > 0
        assert result.metrics.invalid_rows > 0
        assert result.metrics.error_rate > 0

    def test_callback_invoked(self, sample_schema, valid_csv_file: Path) -> None:
        callback_calls: List[ValidationMetrics] = []

        def track_progress(metrics: ValidationMetrics) -> None:
            callback_calls.append(metrics)

        validator = StreamingValidator(sample_schema, chunk_size=2)
        validator.validate_csv_sync(valid_csv_file, report_callback=track_progress)

        assert len(callback_calls) > 0
        assert all(isinstance(m, ValidationMetrics) for m in callback_calls)

    def test_early_termination_on_threshold(
        self, sample_schema, high_error_csv_file: Path
    ) -> None:
        validator = StreamingValidator(
            sample_schema,
            chunk_size=2,
            error_threshold=0.01,
        )
        result = validator.validate_csv_sync(high_error_csv_file)

        assert result.metrics.early_terminated
        assert result.metrics.chunks_processed < 5

    def test_metrics_to_dict(self, sample_schema, valid_csv_file: Path) -> None:
        validator = StreamingValidator(sample_schema, chunk_size=2)
        result = validator.validate_csv_sync(valid_csv_file)

        metrics_dict = result.metrics.to_dict()
        assert "total_rows" in metrics_dict
        assert "valid_rows" in metrics_dict
        assert "error_rate" in metrics_dict
        assert "processing_time" in metrics_dict

    def test_result_to_dict(self, sample_schema, valid_csv_file: Path) -> None:
        validator = StreamingValidator(sample_schema)
        result = validator.validate_csv_sync(valid_csv_file)

        result_dict = result.to_dict()
        assert result_dict["is_valid"] is True
        assert result_dict["schema_name"] == "test_schema"
        assert "metrics" in result_dict


class TestValidateCSVStreamingSync:
    def test_convenience_function(self, sample_schema, valid_csv_file: Path) -> None:
        result = validate_csv_streaming_sync(
            valid_csv_file,
            sample_schema,
            chunk_size=2,
        )

        assert result.is_valid
        assert result.metrics.total_rows == 5


class TestAsyncStreamValidation:
    @pytest.mark.asyncio
    async def test_validate_async_stream(self, sample_schema) -> None:
        async def data_generator() -> AsyncIterator[Dict[str, Any]]:
            for i in range(5):
                yield {"id": i + 1, "name": f"User{i}", "value": float(i * 10)}

        validator = StreamingValidator(sample_schema, chunk_size=2)
        result = await validator.validate_stream(data_generator(), source="test_stream")

        assert result.is_valid
        assert result.metrics.total_rows == 5
        assert result.source == "test_stream"


class TestJSONLValidation:
    def test_validate_jsonl_file(self, sample_schema, tmp_path: Path) -> None:
        filepath = tmp_path / "data.jsonl"
        with open(filepath, "w") as f:
            for i in range(5):
                f.write(f'{{"id": {i + 1}, "name": "User{i}", "value": {i * 10.0}}}\n')

        validator = StreamingValidator(sample_schema, chunk_size=2)
        result = validator.validate_jsonl_sync(filepath)

        assert result.is_valid
        assert result.metrics.total_rows == 5
