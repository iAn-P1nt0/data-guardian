"""Demonstration of data-guardian validation reporting features."""

import pandas as pd
from pathlib import Path

from data_guardian import (
    SchemaBuilder,
    UnifiedValidator,
    ValidationReporter,
    MetricsExporter,
    StreamingValidator,
)

# Email regex pattern
EMAIL_REGEX = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"


def demo_validation_reporting():
    """Demonstrate validation reporting with various output formats."""
    print("=" * 80)
    print("Data Guardian - Validation Reporting Demo")
    print("=" * 80)
    print()

    # 1. Create sample schema
    schema = (
        SchemaBuilder("user_schema")
        .add_column("id", int, nullable=False, unique=True, ge=0)
        .add_column("email", str, nullable=False, pattern=EMAIL_REGEX)
        .add_column("age", int, nullable=False, ge=0, le=120)
        .add_column("score", float, nullable=False, ge=0.0, le=100.0)
        .build()
    )

    print("Schema created with 4 columns: id, email, age, score")
    print()

    # 2. Create sample data with errors
    invalid_data = pd.DataFrame({
        "id": [-1, 2, 2, 4, 5000],  # Negative, duplicate
        "email": ["invalid", "no-at-sign", "test@example.com", "bad@", "x@y.z"],
        "age": [-5, 150, 30, 200, 45],  # Out of range
        "score": [-10.0, 150.0, 78.1, 88.9, 95.0],  # Out of range
    })

    print("Sample data created with multiple validation errors")
    print()

    # 3. Validate data
    validator = UnifiedValidator(schema.to_validation_schema(), lazy=True, auto_fix=True)
    result = validator.validate(invalid_data)

    print(f"Validation complete: {result.is_valid}")
    print(f"Total errors: {len(result.errors)}")
    print(f"Total suggestions: {len(result.suggestions)}")
    print()

    # 4. Generate console report
    print("-" * 80)
    print("CONSOLE REPORT (verbose mode)")
    print("-" * 80)
    reporter = ValidationReporter(result)
    reporter.to_console(verbose=True)
    print()

    # 5. Generate HTML report
    output_dir = Path("output")
    output_dir.mkdir(exist_ok=True)

    html_path = output_dir / "validation_report.html"
    reporter.to_html(html_path, title="User Data Validation Report")
    print(f"HTML report generated: {html_path}")

    # 6. Generate JSON export
    json_path = output_dir / "validation_report.json"
    reporter.to_json(json_path, indent=2)
    print(f"JSON report generated: {json_path}")

    # 7. Convert errors to DataFrame for analysis
    errors_df = reporter.to_dataframe()
    print(f"\nErrors DataFrame shape: {errors_df.shape}")
    print("\nError distribution by column:")
    print(errors_df["column"].value_counts())
    print()

    # 8. Demo metrics export
    print("-" * 80)
    print("METRICS EXPORT DEMO")
    print("-" * 80)

    # Simulate streaming validation metrics
    from data_guardian.core.streaming import ValidationMetrics

    metrics = ValidationMetrics()
    metrics.update(900, 100, ["Error 1", "Error 2", "Error 1"])
    metrics.update(950, 50, ["Error 2", "Error 3"])
    metrics.processing_time = 12.345

    print("\nPrometheus format:")
    print("-" * 40)
    prometheus_output = MetricsExporter.to_prometheus(metrics)
    print(prometheus_output[:500] + "...")

    print("\nOpenTelemetry format:")
    print("-" * 40)
    otel_output = MetricsExporter.to_opentelemetry(metrics)
    print(f"Resource: {otel_output['resource']}")
    print(f"Metrics count: {len(otel_output['metrics'])}")
    print(f"Attributes: {otel_output['attributes']}")

    print()
    print("=" * 80)
    print("Demo complete! Check the 'output' directory for generated reports.")
    print("=" * 80)


if __name__ == "__main__":
    demo_validation_reporting()
