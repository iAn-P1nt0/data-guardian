"""Performance benchmark tests using pytest-benchmark."""

from __future__ import annotations

import pandas as pd
import pytest

from pandera_unified_validator import SchemaBuilder, UnifiedValidator


@pytest.fixture()
def small_dataframe() -> pd.DataFrame:
    """Small DataFrame for benchmarking (100 rows)."""
    return pd.DataFrame({
        "id": range(100),
        "name": [f"user_{i}" for i in range(100)],
        "age": [25 + (i % 50) for i in range(100)],
        "score": [float(50 + (i % 50)) for i in range(100)],
        "active": [i % 2 == 0 for i in range(100)],
    })


@pytest.fixture()
def medium_dataframe() -> pd.DataFrame:
    """Medium DataFrame for benchmarking (10,000 rows)."""
    return pd.DataFrame({
        "id": range(10000),
        "name": [f"user_{i}" for i in range(10000)],
        "age": [25 + (i % 50) for i in range(10000)],
        "score": [float(50 + (i % 50)) for i in range(10000)],
        "active": [i % 2 == 0 for i in range(10000)],
    })


@pytest.fixture()
def large_dataframe() -> pd.DataFrame:
    """Large DataFrame for benchmarking (100,000 rows)."""
    return pd.DataFrame({
        "id": range(100000),
        "name": [f"user_{i}" for i in range(100000)],
        "age": [25 + (i % 50) for i in range(100000)],
        "score": [float(50 + (i % 50)) for i in range(100000)],
        "active": [i % 2 == 0 for i in range(100000)],
    })


@pytest.fixture()
def benchmark_schema() -> SchemaBuilder:
    """Standard schema for benchmarking."""
    return (
        SchemaBuilder("benchmark_schema")
        .add_column("id", int, nullable=False, ge=0)
        .add_column("name", str, nullable=False)
        .add_column("age", int, nullable=False, ge=0, le=120)
        .add_column("score", float, nullable=False, ge=0.0, le=100.0)
        .add_column("active", bool, nullable=False)
        .build()
    )


class TestValidationBenchmarks:
    """Benchmark tests for validation performance."""

    def test_benchmark_small_dataframe_validation(
        self,
        benchmark: pytest.FixtureRequest,
        small_dataframe: pd.DataFrame,
        benchmark_schema: SchemaBuilder,
    ) -> None:
        """Benchmark validation of small DataFrame (100 rows)."""
        validator = UnifiedValidator(benchmark_schema.to_validation_schema(), lazy=True)

        result = benchmark(validator.validate, small_dataframe)
        assert result.is_valid is True

    def test_benchmark_medium_dataframe_validation(
        self,
        benchmark: pytest.FixtureRequest,
        medium_dataframe: pd.DataFrame,
        benchmark_schema: SchemaBuilder,
    ) -> None:
        """Benchmark validation of medium DataFrame (10,000 rows)."""
        validator = UnifiedValidator(benchmark_schema.to_validation_schema(), lazy=True)

        result = benchmark(validator.validate, medium_dataframe)
        assert result.is_valid is True

    def test_benchmark_large_dataframe_validation(
        self,
        benchmark: pytest.FixtureRequest,
        large_dataframe: pd.DataFrame,
        benchmark_schema: SchemaBuilder,
    ) -> None:
        """Benchmark validation of large DataFrame (100,000 rows)."""
        validator = UnifiedValidator(benchmark_schema.to_validation_schema(), lazy=True)

        result = benchmark(validator.validate, large_dataframe)
        assert result.is_valid is True

    def test_benchmark_schema_creation(
        self,
        benchmark: pytest.FixtureRequest,
    ) -> None:
        """Benchmark schema creation performance."""

        def create_schema() -> SchemaBuilder:
            return (
                SchemaBuilder("test_schema")
                .add_column("col1", int, nullable=False, ge=0, le=1000)
                .add_column("col2", str, nullable=False)
                .add_column("col3", float, nullable=False, ge=0.0, le=100.0)
                .add_column("col4", bool, nullable=False)
                .add_column("col5", int, nullable=True)
                .build()
            )

        schema = benchmark(create_schema)
        assert len(schema.columns) == 5

    def test_benchmark_validator_initialization(
        self,
        benchmark: pytest.FixtureRequest,
        benchmark_schema: SchemaBuilder,
    ) -> None:
        """Benchmark validator initialization performance."""

        def create_validator() -> UnifiedValidator:
            return UnifiedValidator(benchmark_schema.to_validation_schema(), lazy=True)

        validator = benchmark(create_validator)
        assert validator is not None

    def test_benchmark_auto_fix_suggestions(
        self,
        benchmark: pytest.FixtureRequest,
        medium_dataframe: pd.DataFrame,
        benchmark_schema: SchemaBuilder,
    ) -> None:
        """Benchmark auto-fix suggestion generation."""
        # Create invalid data
        invalid_df = medium_dataframe.copy()
        invalid_df.loc[0, "age"] = -1
        invalid_df.loc[1, "score"] = 150.0

        validator = UnifiedValidator(
            benchmark_schema.to_validation_schema(),
            lazy=True,
            auto_fix=True,
        )

        result = benchmark(validator.validate, invalid_df)
        assert not result.is_valid
        assert len(result.suggestions) > 0


class TestSchemaOperationBenchmarks:
    """Benchmark tests for schema operations."""

    def test_benchmark_schema_to_pandera(
        self,
        benchmark: pytest.FixtureRequest,
        benchmark_schema: SchemaBuilder,
    ) -> None:
        """Benchmark schema conversion to Pandera."""
        result = benchmark(benchmark_schema.to_pandera)
        assert result is not None

    def test_benchmark_schema_to_pydantic(
        self,
        benchmark: pytest.FixtureRequest,
        benchmark_schema: SchemaBuilder,
    ) -> None:
        """Benchmark schema conversion to Pydantic."""
        result = benchmark(benchmark_schema.to_pydantic)
        assert result is not None

    def test_benchmark_schema_to_dict(
        self,
        benchmark: pytest.FixtureRequest,
        benchmark_schema: SchemaBuilder,
    ) -> None:
        """Benchmark schema serialization to dict."""
        result = benchmark(benchmark_schema.to_dict)
        assert isinstance(result, dict)

    def test_benchmark_schema_to_json(
        self,
        benchmark: pytest.FixtureRequest,
        benchmark_schema: SchemaBuilder,
    ) -> None:
        """Benchmark schema serialization to JSON."""
        result = benchmark(benchmark_schema.to_json)
        assert isinstance(result, str)


class TestReportingBenchmarks:
    """Benchmark tests for reporting operations."""

    def test_benchmark_reporter_to_dataframe(
        self,
        benchmark: pytest.FixtureRequest,
        invalid_sample_dataframe: pd.DataFrame,
        unified_validator: UnifiedValidator,
    ) -> None:
        """Benchmark reporter DataFrame conversion."""
        from pandera_unified_validator.utils.reporting import ValidationReporter

        result = unified_validator.validate(invalid_sample_dataframe)
        reporter = ValidationReporter(result)

        df = benchmark(reporter.to_dataframe)
        assert isinstance(df, pd.DataFrame)

    def test_benchmark_metrics_to_prometheus(
        self,
        benchmark: pytest.FixtureRequest,
    ) -> None:
        """Benchmark Prometheus metrics export."""
        from pandera_unified_validator.core.streaming import ValidationMetrics
        from pandera_unified_validator.utils.reporting import MetricsExporter

        metrics = ValidationMetrics()
        metrics.update(9000, 1000, ["Error 1"] * 100)

        result = benchmark(MetricsExporter.to_prometheus, metrics)
        assert isinstance(result, str)

    def test_benchmark_metrics_to_opentelemetry(
        self,
        benchmark: pytest.FixtureRequest,
    ) -> None:
        """Benchmark OpenTelemetry metrics export."""
        from pandera_unified_validator.core.streaming import ValidationMetrics
        from pandera_unified_validator.utils.reporting import MetricsExporter

        metrics = ValidationMetrics()
        metrics.update(9000, 1000, ["Error 1"] * 100)

        result = benchmark(MetricsExporter.to_opentelemetry, metrics)
        assert isinstance(result, dict)


class TestDataFrameConversionBenchmarks:
    """Benchmark tests for data conversion operations."""

    def test_benchmark_dict_to_dataframe_conversion(
        self,
        benchmark: pytest.FixtureRequest,
        benchmark_schema: SchemaBuilder,
    ) -> None:
        """Benchmark dict to DataFrame conversion in validator."""
        records = [
            {
                "id": i,
                "name": f"user_{i}",
                "age": 25 + (i % 50),
                "score": float(50 + (i % 50)),
                "active": i % 2 == 0,
            }
            for i in range(1000)
        ]

        validator = UnifiedValidator(benchmark_schema.to_validation_schema(), lazy=True)

        result = benchmark(validator.validate, records)
        assert result.is_valid is True

    def test_benchmark_single_record_validation(
        self,
        benchmark: pytest.FixtureRequest,
        benchmark_schema: SchemaBuilder,
    ) -> None:
        """Benchmark validation of single record."""
        record = {
            "id": 1,
            "name": "test_user",
            "age": 30,
            "score": 85.5,
            "active": True,
        }

        validator = UnifiedValidator(benchmark_schema.to_validation_schema(), lazy=True)

        result = benchmark(validator.validate, record)
        assert result.is_valid is True
