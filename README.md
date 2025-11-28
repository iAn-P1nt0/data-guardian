# pandera-unified-validator

[![PyPI version](https://badge.fury.io/py/pandera-unified-validator.svg)](https://badge.fury.io/py/pandera-unified-validator)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![CI Status](https://github.com/ianpinto/pandera-unified-validator/workflows/CI/badge.svg)](https://github.com/ianpinto/pandera-unified-validator/actions)
[![Coverage](https://codecov.io/gh/ianpinto/pandera-unified-validator/branch/main/graph/badge.svg)](https://codecov.io/gh/ianpinto/pandera-unified-validator)

**Advanced data validation library unifying Pydantic and Pandera with multi-backend support for pandas and Polars.**

## Features

- 🔐 **Unified Validation** – Single schema for both record-level (Pydantic) and DataFrame-level (Pandera) validation
- ⚡ **Multi-Backend Support** – Seamlessly switch between pandas and Polars without rewriting validation rules
- 📊 **Streaming Validation** – Efficiently validate large CSV, Parquet, and JSONL files that don't fit in memory
- 🔧 **Auto-Fix Suggestions** – Intelligent suggestions for common data quality issues with one-click fixes
- 📈 **Data Profiling** – Generate statistical profiles and infer validation constraints automatically
- 📝 **Rich Reporting** – Beautiful console output, interactive HTML reports, and metrics export (Prometheus, OpenTelemetry)
- 🧪 **Type-Safe** – Full type hints with mypy strict mode support
- 🚀 **Production Ready** – Comprehensive test suite with >90% coverage, property-based testing, and benchmarks

## Installation

```bash
pip install pandera-unified-validator
```

With optional dependencies:

```bash
# For Parquet support
pip install pandera-unified-validator[parquet]

# For database validation
pip install pandera-unified-validator[database]

# For data profiling
pip install pandera-unified-validator[profiling]

# All features
pip install pandera-unified-validator[all]
```

## Quick Start (30 seconds)

```python
import pandas as pd
from data_guardian import SchemaBuilder, UnifiedValidator

# Define schema with fluent API
schema = (
    SchemaBuilder("user_schema")
    .add_column("user_id", int, unique=True, ge=0)
    .add_column("email", str, pattern=r"^[\w\.-]+@[\w\.-]+\.\w+$")
    .add_column("age", int, ge=0, le=120)
    .add_column("score", float, ge=0.0, le=100.0)
    .build()
)

# Create validator with auto-fix enabled
validator = UnifiedValidator(schema.to_validation_schema(), auto_fix=True)

# Validate your data
data = pd.DataFrame({
    "user_id": [1, 2, 3],
    "email": ["user@example.com", "invalid-email", "admin@test.org"],
    "age": [25, 150, 30],  # 150 is out of range
    "score": [85.5, 92.0, 78.5]
})

result = validator.validate(data)

# Check results
print(f"Valid: {result.is_valid}")
print(f"Errors: {len(result.errors)}")
print(f"Suggestions: {len(result.suggestions)}")

# Generate beautiful reports
from data_guardian import ValidationReporter

reporter = ValidationReporter(result)
reporter.to_console(verbose=True)  # Rich console output
reporter.to_html("report.html")     # Interactive HTML report
reporter.to_json("report.json")     # JSON export
```

## Comparison with Alternatives

| Feature | pandera-unified-validator | Pydantic | Pandera |
|---------|---------------|----------|---------|
| Record validation | ✅ | ✅ | ❌ |
| DataFrame validation | ✅ | ❌ | ✅ |
| Unified schema | ✅ | ❌ | ❌ |
| Multi-backend (pandas/Polars) | ✅ | ❌ | ❌ |
| Streaming validation | ✅ | ❌ | ❌ |
| Auto-fix suggestions | ✅ | ❌ | ❌ |
| Data profiling | ✅ | ❌ | ✅ |
| HTML/JSON reports | ✅ | ❌ | ❌ |
| Metrics export | ✅ | ❌ | ❌ |

## Real-World Example: E-commerce Product Validation

```python
from data_guardian import UnifiedValidator, SchemaBuilder, ValidationReporter

# Define comprehensive product schema
schema = (
    SchemaBuilder("product_catalog")
    .add_column("product_id", str, unique=True, pattern=r"^PRD-\d{6}$")
    .add_column("name", str, nullable=False)
    .add_column("price", float, ge=0.01, le=1_000_000)
    .add_column("category", str, isin=["Electronics", "Clothing", "Books", "Home"])
    .add_column("stock_quantity", int, ge=0)
    .add_column("supplier_id", str, pattern=r"^SUP-\d{4}$")
    .add_cross_column_constraint(
        "price_check",
        ["price", "category"],
        lambda df: df["price"] < 10000 if df["category"] == "Books" else True,
        error_message="Books must be priced under $10,000"
    )
    .build()
)

# Validate with auto-fix
validator = UnifiedValidator(schema.to_validation_schema(), auto_fix=True)
result = validator.validate(products_df)

# Generate comprehensive report
reporter = ValidationReporter(result)
reporter.to_console(verbose=True)
reporter.to_html("validation_report.html")

# Apply auto-fixes
if result.suggestions:
    fixed_df = validator.apply_fixes(products_df, result)
    print(f"Fixed {len(result.suggestions)} issues automatically")
```

## Streaming Validation for Large Files

```python
from data_guardian import StreamingValidator

# Validate large CSV without loading into memory
schema = SchemaBuilder("transactions").add_column("amount", float, ge=0).build()
validator = StreamingValidator(schema, chunk_size=10000, error_threshold=0.05)

# Async validation with progress callback
async def progress_callback(metrics):
    print(f"Processed {metrics.total_rows} rows, {metrics.error_rate:.2%} error rate")

result = await validator.validate_csv(
    "large_transactions.csv",
    report_callback=progress_callback
)

print(f"Total rows: {result.metrics.total_rows}")
print(f"Invalid rows: {result.metrics.invalid_rows}")
print(f"Processing time: {result.metrics.processing_time:.2f}s")
```

## Documentation

- **[User Guide](docs/user_guide.md)** - Complete tutorial and API reference
- **[Examples](docs/examples/)** - 9 practical examples covering common use cases
- **[API Documentation](https://pandera-unified-validator.readthedocs.io/)** - Auto-generated API docs
- **[Contributing Guide](CONTRIBUTING.md)** - How to contribute to the project

## Development

```bash
# Clone repository
git clone https://github.com/ianpinto/pandera-unified-validator.git
cd pandera-unified-validator

# Install development dependencies
pip install -e ".[dev]"

# Run tests
pytest tests/ -v --cov=src/data_guardian

# Run linting
ruff check src/ tests/

# Run type checking
mypy src/

# Run formatting
black src/ tests/

# Run all checks
ruff check src/ && black --check src/ && mypy src/ && pytest
```

## Contributing

Contributions are welcome! Please read our [Contributing Guide](CONTRIBUTING.md) for details on:

- Code of conduct
- Development setup
- Testing requirements
- Code style guidelines
- Pull request process

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Built on top of [Pydantic](https://pydantic-docs.helpmanual.io/) and [Pandera](https://pandera.readthedocs.io/)
- Inspired by the need for unified data validation in production data pipelines
- Thanks to all [contributors](https://github.com/ianpinto/pandera-unified-validator/graphs/contributors)

## Citation

If you use pandera-unified-validator in your research or production systems, please cite:

```bibtex
@software{data_guardian,
  title = {pandera-unified-validator: Advanced data validation library},
  author = {Ian Pinto},
  year = {2024},
  url = {https://github.com/ianpinto/pandera-unified-validator}
}
```
