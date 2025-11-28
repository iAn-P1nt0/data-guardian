"""Tests for validation reporting functionality."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest
from rich.console import Console

from pandera_unified_validator import UnifiedValidator, ValidationResult
from pandera_unified_validator.core.streaming import ValidationMetrics
from pandera_unified_validator.core.validator import AutoFixSuggestion, ValidationErrorDetail
from pandera_unified_validator.utils.reporting import MetricsExporter, ValidationReporter


@pytest.fixture()
def validation_result_with_errors() -> ValidationResult:
    """Create a validation result with errors for testing."""
    return ValidationResult(
        is_valid=False,
        errors=[
            ValidationErrorDetail(message="Email validation failed", row=1, column="email"),
            ValidationErrorDetail(message="Age must be >= 0", row=3, column="age"),
            ValidationErrorDetail(message="Age must be <= 120", row=1, column="age"),
            ValidationErrorDetail(message="Email validation failed", row=4, column="email"),
            ValidationErrorDetail(message="ID must be unique", row=2, column="id"),
        ],
        warnings=["Warning: Null values detected in optional column"],
        suggestions=[
            AutoFixSuggestion(
                description="Coerce column 'age' to the expected dtype",
                column="age",
            ),
            AutoFixSuggestion(
                description="Validate email format for column 'email'",
                column="email",
            ),
        ],
        metadata={"total_rows": 5, "stage": "validation"},
    )


@pytest.fixture()
def validation_result_valid() -> ValidationResult:
    """Create a valid validation result for testing."""
    return ValidationResult(
        is_valid=True,
        errors=[],
        warnings=[],
        suggestions=[],
        metadata={"total_rows": 100, "stage": "validation"},
    )


@pytest.fixture()
def validation_metrics() -> ValidationMetrics:
    """Create validation metrics for testing."""
    metrics = ValidationMetrics()
    metrics.update(900, 100, ["Error 1", "Error 2", "Error 1"])
    metrics.update(950, 50, ["Error 2", "Error 3"])
    metrics.processing_time = 12.345
    return metrics


class TestValidationReporter:
    """Tests for ValidationReporter class."""

    def test_init(self, validation_result_with_errors: ValidationResult) -> None:
        """Test reporter initialization."""
        reporter = ValidationReporter(validation_result_with_errors)
        assert reporter.result == validation_result_with_errors

    def test_to_console_with_errors(
        self,
        validation_result_with_errors: ValidationResult,
        capsys: pytest.CaptureFixture,
    ) -> None:
        """Test console output with errors."""
        reporter = ValidationReporter(validation_result_with_errors)
        console = Console(file=None, force_terminal=False)

        # Should not raise
        reporter.to_console(verbose=False, console=console)

        # Verify it creates a reporter (actual console output testing is complex)
        assert reporter.result.is_valid is False

    def test_to_console_verbose(
        self,
        validation_result_with_errors: ValidationResult,
    ) -> None:
        """Test verbose console output."""
        reporter = ValidationReporter(validation_result_with_errors)
        console = Console(file=None, force_terminal=False)

        # Should not raise in verbose mode
        reporter.to_console(verbose=True, console=console)

        assert len(reporter.result.errors) == 5

    def test_to_console_valid_result(
        self,
        validation_result_valid: ValidationResult,
    ) -> None:
        """Test console output with valid result."""
        reporter = ValidationReporter(validation_result_valid)
        console = Console(file=None, force_terminal=False)

        # Should not raise
        reporter.to_console(verbose=False, console=console)

        assert reporter.result.is_valid is True

    def test_to_html(
        self,
        validation_result_with_errors: ValidationResult,
        tmp_path: Path,
    ) -> None:
        """Test HTML report generation."""
        reporter = ValidationReporter(validation_result_with_errors)
        output_file = tmp_path / "report.html"

        reporter.to_html(output_file, title="Test Validation Report")

        assert output_file.exists()
        content = output_file.read_text()
        assert "Test Validation Report" in content
        assert "INVALID" in content
        assert "Total Errors" in content
        assert "chart.js" in content

    def test_to_html_valid_result(
        self,
        validation_result_valid: ValidationResult,
        tmp_path: Path,
    ) -> None:
        """Test HTML report generation for valid result."""
        reporter = ValidationReporter(validation_result_valid)
        output_file = tmp_path / "report_valid.html"

        reporter.to_html(output_file, title="Valid Data Report")

        assert output_file.exists()
        content = output_file.read_text()
        assert "Valid Data Report" in content
        assert "VALID" in content

    def test_to_json(
        self,
        validation_result_with_errors: ValidationResult,
        tmp_path: Path,
    ) -> None:
        """Test JSON export."""
        reporter = ValidationReporter(validation_result_with_errors)
        output_file = tmp_path / "report.json"

        reporter.to_json(output_file, indent=2)

        assert output_file.exists()
        data = json.loads(output_file.read_text())

        assert data["is_valid"] is False
        assert data["summary"]["total_errors"] == 5
        assert data["summary"]["total_warnings"] == 1
        assert data["summary"]["total_suggestions"] == 2
        assert len(data["errors"]) == 5
        assert len(data["warnings"]) == 1
        assert len(data["suggestions"]) == 2
        assert "timestamp" in data

    def test_to_json_valid_result(
        self,
        validation_result_valid: ValidationResult,
        tmp_path: Path,
    ) -> None:
        """Test JSON export for valid result."""
        reporter = ValidationReporter(validation_result_valid)
        output_file = tmp_path / "report_valid.json"

        reporter.to_json(output_file)

        data = json.loads(output_file.read_text())
        assert data["is_valid"] is True
        assert data["summary"]["total_errors"] == 0

    def test_to_dataframe_with_errors(
        self,
        validation_result_with_errors: ValidationResult,
    ) -> None:
        """Test DataFrame conversion with errors."""
        reporter = ValidationReporter(validation_result_with_errors)
        df = reporter.to_dataframe()

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 5
        assert list(df.columns) == ["row", "column", "message", "context_keys"]
        assert df["column"].tolist() == ["email", "age", "age", "email", "id"]
        assert df["row"].tolist() == [1, 3, 1, 4, 2]

    def test_to_dataframe_no_errors(
        self,
        validation_result_valid: ValidationResult,
    ) -> None:
        """Test DataFrame conversion with no errors."""
        reporter = ValidationReporter(validation_result_valid)
        df = reporter.to_dataframe()

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0
        assert list(df.columns) == ["row", "column", "message", "context_keys"]

    def test_group_errors_by_column(
        self,
        validation_result_with_errors: ValidationResult,
    ) -> None:
        """Test error grouping by column."""
        reporter = ValidationReporter(validation_result_with_errors)
        grouped = reporter._group_errors_by_column()

        assert grouped["email"] == 2
        assert grouped["age"] == 2
        assert grouped["id"] == 1


class TestMetricsExporter:
    """Tests for MetricsExporter class."""

    def test_to_prometheus(self, validation_metrics: ValidationMetrics) -> None:
        """Test Prometheus format export."""
        output = MetricsExporter.to_prometheus(validation_metrics)

        assert "puv_total_rows 2000" in output
        assert "puv_valid_rows 1850" in output
        assert "puv_invalid_rows 150" in output
        assert "puv_error_rate" in output
        assert "puv_processing_time_seconds 12.345" in output
        assert "puv_chunks_processed 2" in output
        assert "# HELP" in output
        assert "# TYPE" in output

    def test_to_prometheus_common_errors(
        self,
        validation_metrics: ValidationMetrics,
    ) -> None:
        """Test Prometheus export includes common errors."""
        output = MetricsExporter.to_prometheus(validation_metrics)

        # Should include common errors as labeled metrics
        assert "puv_common_errors" in output

    def test_to_opentelemetry(self, validation_metrics: ValidationMetrics) -> None:
        """Test OpenTelemetry format export."""
        output = MetricsExporter.to_opentelemetry(validation_metrics)

        assert isinstance(output, dict)
        assert output["resource"]["service.name"] == "pandera-unified-validator"
        assert output["resource"]["service.version"] == "0.1.0"

        metrics = output["metrics"]
        assert len(metrics) == 6

        # Verify metric names and values
        metric_names = {m["name"] for m in metrics}
        assert "puv.total_rows" in metric_names
        assert "puv.valid_rows" in metric_names
        assert "puv.invalid_rows" in metric_names
        assert "puv.error_rate" in metric_names
        assert "puv.processing_time" in metric_names
        assert "puv.chunks_processed" in metric_names

        # Check attributes
        assert "early_terminated" in output["attributes"]
        assert "common_errors_count" in output["attributes"]
        assert "timestamp" in output

    def test_to_opentelemetry_structure(
        self,
        validation_metrics: ValidationMetrics,
    ) -> None:
        """Test OpenTelemetry output structure."""
        output = MetricsExporter.to_opentelemetry(validation_metrics)

        # Verify metric structure
        for metric in output["metrics"]:
            assert "name" in metric
            assert "description" in metric
            assert "unit" in metric
            assert "type" in metric
            assert "value" in metric


class TestIntegrationReporting:
    """Integration tests for reporting with real validation."""

    def test_reporter_with_unified_validator(
        self,
        invalid_sample_dataframe: pd.DataFrame,
        unified_validator: UnifiedValidator,
        tmp_path: Path,
    ) -> None:
        """Test reporter with real validation result."""
        result = unified_validator.validate(invalid_sample_dataframe)
        reporter = ValidationReporter(result)

        # Test all export formats
        html_file = tmp_path / "integration_report.html"
        json_file = tmp_path / "integration_report.json"

        reporter.to_html(html_file)
        reporter.to_json(json_file)

        assert html_file.exists()
        assert json_file.exists()

        # Verify JSON content
        data = json.loads(json_file.read_text())
        assert data["is_valid"] is False
        assert data["summary"]["total_errors"] > 0

    def test_reporter_dataframe_analysis(
        self,
        invalid_sample_dataframe: pd.DataFrame,
        unified_validator: UnifiedValidator,
    ) -> None:
        """Test DataFrame analysis of validation errors."""
        result = unified_validator.validate(invalid_sample_dataframe)
        reporter = ValidationReporter(result)

        df = reporter.to_dataframe()
        assert len(df) > 0

        # Analyze error distribution
        error_counts = df["column"].value_counts()
        assert len(error_counts) > 0
