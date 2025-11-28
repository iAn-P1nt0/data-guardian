"""
Complete Integration Example: E-commerce Order Processing Pipeline

This example demonstrates all major features of pandera-unified-validator in a realistic
production-like scenario for validating e-commerce order data.
"""

import asyncio
from pathlib import Path

import pandas as pd

from pandera_unified_validator import (
    SchemaBuilder,
    StreamingValidator,
    UnifiedValidator,
    ValidationReporter,
    configure_logging,
    get_logger,
)
from pandera_unified_validator.profiling import DataProfiler, infer_constraints_from_profile

# Configure logging
configure_logging(level="INFO", json_logs=False, include_timestamp=True)
logger = get_logger(__name__)


def create_order_schema():
    """Create a comprehensive order validation schema."""
    logger.info("schema_creation_started")

    schema = (
        SchemaBuilder("ecommerce_orders")
        .add_column(
            "order_id",
            str,
            nullable=False,
            unique=True,
            pattern=r"^ORD-\d{8}$",
            description="Unique order identifier",
        )
        .add_column(
            "customer_id",
            str,
            nullable=False,
            pattern=r"^CUST-\d{6}$",
            description="Customer identifier",
        )
        .add_column(
            "product_id",
            str,
            nullable=False,
            pattern=r"^PROD-\d{5}$",
            description="Product identifier",
        )
        .add_column(
            "quantity",
            int,
            nullable=False,
            ge=1,
            le=1000,
            description="Order quantity",
        )
        .add_column(
            "unit_price",
            float,
            nullable=False,
            ge=0.01,
            le=100000.0,
            description="Price per unit",
        )
        .add_column(
            "total_amount",
            float,
            nullable=False,
            ge=0.01,
            description="Total order amount",
        )
        .add_column(
            "status",
            str,
            nullable=False,
            isin=["pending", "processing", "shipped", "delivered", "cancelled"],
            description="Order status",
        )
        .add_column(
            "payment_method",
            str,
            nullable=False,
            isin=["credit_card", "debit_card", "paypal", "bank_transfer"],
            description="Payment method",
        )
        .with_metadata(
            version="1.0",
            description="E-commerce order validation schema",
            owner="data-team",
        )
        .build()
    )

    logger.info(
        "schema_created",
        columns=len(schema.columns),
        metadata=schema.metadata,
    )
    return schema


def create_sample_data():
    """Create sample order data with some intentional errors."""
    logger.info("sample_data_generation_started")

    data = pd.DataFrame({
        "order_id": [
            "ORD-00000001",
            "ORD-00000002",
            "INVALID-ID",  # Invalid format
            "ORD-00000004",
            "ORD-00000005",
        ],
        "customer_id": [
            "CUST-123456",
            "CUST-123457",
            "CUST-123458",
            "CUST-123459",
            "CUST-123460",
        ],
        "product_id": [
            "PROD-10001",
            "PROD-10002",
            "PROD-10003",
            "PROD-10004",
            "PROD-10005",
        ],
        "quantity": [2, 1, 1500, 3, 1],  # 1500 exceeds max
        "unit_price": [29.99, 149.99, 79.99, 199.99, 9.99],
        "total_amount": [59.98, 149.99, 119985.00, 599.97, 9.99],
        "status": [
            "pending",
            "shipped",
            "invalid_status",  # Invalid status
            "delivered",
            "pending",
        ],
        "payment_method": [
            "credit_card",
            "paypal",
            "credit_card",
            "bank_transfer",
            "debit_card",
        ],
    })

    logger.info("sample_data_generated", rows=len(data), columns=len(data.columns))
    return data


def validate_orders_in_memory(schema, data):
    """Validate orders using in-memory validation with auto-fix."""
    logger.info("in_memory_validation_started", rows=len(data))

    # Create validator with auto-fix
    validator = UnifiedValidator(
        schema.to_validation_schema(),
        lazy=True,
        auto_fix=True,
    )

    # Validate
    result = validator.validate(data)

    logger.info(
        "validation_completed",
        is_valid=result.is_valid,
        errors=len(result.errors),
        warnings=len(result.warnings),
        suggestions=len(result.suggestions),
    )

    # Generate reports
    reporter = ValidationReporter(result)

    # Console report
    print("\n" + "=" * 80)
    print("IN-MEMORY VALIDATION RESULTS")
    print("=" * 80)
    reporter.to_console(verbose=True)

    # Export reports
    output_dir = Path("output")
    output_dir.mkdir(exist_ok=True)

    reporter.to_html(output_dir / "order_validation.html")
    reporter.to_json(output_dir / "order_validation.json")

    logger.info(
        "reports_generated",
        html="output/order_validation.html",
        json="output/order_validation.json",
    )

    # Show error analysis
    errors_df = reporter.to_dataframe()
    if not errors_df.empty:
        print("\n" + "-" * 80)
        print("ERROR ANALYSIS")
        print("-" * 80)
        print("\nErrors by column:")
        print(errors_df["column"].value_counts())

    return result


async def validate_orders_streaming(schema):
    """Validate orders using streaming validation for large datasets."""
    logger.info("streaming_validation_started")

    # Create sample CSV for streaming validation
    output_dir = Path("output")
    output_dir.mkdir(exist_ok=True)
    csv_path = output_dir / "large_orders.csv"

    # Generate larger dataset
    large_data = create_sample_data()
    # Repeat data to simulate larger file
    large_data = pd.concat([large_data] * 100, ignore_index=True)
    large_data.to_csv(csv_path, index=False)

    logger.info("test_csv_created", path=str(csv_path), rows=len(large_data))

    # Progress callback
    def progress_callback(metrics):
        logger.info(
            "streaming_progress",
            total_rows=metrics.total_rows,
            valid_rows=metrics.valid_rows,
            invalid_rows=metrics.invalid_rows,
            error_rate=f"{metrics.error_rate:.2%}",
        )

    # Create streaming validator
    validator = StreamingValidator(
        schema,
        chunk_size=100,
        error_threshold=0.1,
    )

    # Validate
    result = await validator.validate_csv(
        csv_path,
        report_callback=progress_callback,
    )

    logger.info(
        "streaming_validation_completed",
        is_valid=result.is_valid,
        total_rows=result.metrics.total_rows,
        invalid_rows=result.metrics.invalid_rows,
        processing_time=result.metrics.processing_time,
    )

    print("\n" + "=" * 80)
    print("STREAMING VALIDATION RESULTS")
    print("=" * 80)
    print(f"Valid: {result.is_valid}")
    print(f"Total rows: {result.metrics.total_rows:,}")
    print(f"Valid rows: {result.metrics.valid_rows:,}")
    print(f"Invalid rows: {result.metrics.invalid_rows:,}")
    print(f"Error rate: {result.metrics.error_rate:.2%}")
    print(f"Processing time: {result.metrics.processing_time:.2f}s")
    print(f"Chunks processed: {result.metrics.chunks_processed}")

    # Show common errors
    if result.metrics.common_errors:
        print("\nTop 5 Common Errors:")
        for error, count in list(result.metrics.common_errors.items())[:5]:
            print(f"  • {error[:80]}: {count} occurrences")

    return result


def profile_and_infer_schema(data):
    """Generate data profile and infer validation schema."""
    logger.info("profiling_started", rows=len(data))

    # Create profiler
    profiler = DataProfiler()

    # Generate profile
    profile = profiler.profile(data, minimal=True)

    logger.info(
        "profile_generated",
        quality_score=profile.quality_score.overall,
        completeness=profile.quality_score.completeness,
        uniqueness=profile.quality_score.uniqueness,
    )

    print("\n" + "=" * 80)
    print("DATA PROFILING RESULTS")
    print("=" * 80)
    print(f"Quality Score: {profile.quality_score.overall:.2%}")
    print(f"Completeness: {profile.quality_score.completeness:.2%}")
    print(f"Uniqueness: {profile.quality_score.uniqueness:.2%}")
    print(f"Consistency: {profile.quality_score.consistency:.2%}")

    # Export profile
    output_dir = Path("output")
    profiler.to_html(profile, output_dir / "order_profile.html")
    logger.info("profile_exported", path="output/order_profile.html")

    # Infer schema
    inferred_schema = infer_constraints_from_profile(profile)
    logger.info("schema_inferred", columns=len(inferred_schema.columns))

    print("\nInferred Schema:")
    print("-" * 80)
    for col_name, col_spec in list(inferred_schema.columns.items())[:5]:
        print(f"  • {col_name}: {col_spec.dtype}")

    return profile, inferred_schema


async def main():
    """Run complete integration example."""
    logger.info("integration_example_started")

    print("=" * 80)
    print("PANDERA-UNIFIED-VALIDATOR COMPLETE INTEGRATION EXAMPLE")
    print("=" * 80)

    # Step 1: Create schema
    print("\n[1] Creating validation schema...")
    schema = create_order_schema()

    # Step 2: Generate sample data
    print("\n[2] Generating sample data...")
    data = create_sample_data()

    # Step 3: In-memory validation
    print("\n[3] Running in-memory validation...")
    result = validate_orders_in_memory(schema, data)

    # Step 4: Streaming validation
    print("\n[4] Running streaming validation...")
    streaming_result = await validate_orders_streaming(schema)

    # Step 5: Data profiling
    print("\n[5] Generating data profile...")
    profile, inferred_schema = profile_and_infer_schema(data)

    # Step 6: Export metrics
    print("\n[6] Exporting metrics...")
    from pandera_unified_validator import MetricsExporter

    prometheus_metrics = MetricsExporter.to_prometheus(streaming_result.metrics)
    output_dir = Path("output")
    (output_dir / "metrics.prom").write_text(prometheus_metrics)
    logger.info("metrics_exported", path="output/metrics.prom")

    otel_metrics = MetricsExporter.to_opentelemetry(streaming_result.metrics)
    (output_dir / "metrics.json").write_text(
        pd.Series(otel_metrics).to_json(indent=2)
    )

    print("\n" + "=" * 80)
    print("INTEGRATION EXAMPLE COMPLETED")
    print("=" * 80)
    print("\nGenerated files in output/:")
    print("  • order_validation.html - Validation report")
    print("  • order_validation.json - Validation data")
    print("  • order_profile.html - Data profile report")
    print("  • large_orders.csv - Test dataset")
    print("  • metrics.prom - Prometheus metrics")
    print("  • metrics.json - OpenTelemetry metrics")

    logger.info("integration_example_completed")


if __name__ == "__main__":
    asyncio.run(main())
