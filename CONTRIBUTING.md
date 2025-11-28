# Contributing to pandera-unified-validator

Thank you for your interest in contributing to pandera-unified-validator! This document provides guidelines and instructions for contributing to the project.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Development Setup](#development-setup)
- [Development Workflow](#development-workflow)
- [Testing](#testing)
- [Code Style](#code-style)
- [Pull Request Process](#pull-request-process)
- [Reporting Issues](#reporting-issues)

## Code of Conduct

By participating in this project, you agree to maintain a respectful and inclusive environment for all contributors.

## Getting Started

1. Fork the repository on GitHub
2. Clone your fork locally:
   ```bash
   git clone https://github.com/YOUR-USERNAME/pandera-unified-validator.git
   cd pandera-unified-validator
   ```

3. Add the upstream repository:
   ```bash
   git remote add upstream https://github.com/ianpinto/pandera-unified-validator.git
   ```

## Development Setup

### Prerequisites

- Python 3.10 or higher
- Git
- pip

### Installation

1. Create a virtual environment:
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

2. Install development dependencies:
   ```bash
   pip install -e ".[dev,all]"
   ```

3. Install pre-commit hooks:
   ```bash
   pip install pre-commit
   pre-commit install
   ```

## Development Workflow

### Creating a Branch

Create a new branch for your feature or bug fix:

```bash
git checkout -b feature/your-feature-name
# or
git checkout -b fix/your-bug-fix
```

Branch naming conventions:
- `feature/` - for new features
- `fix/` - for bug fixes
- `docs/` - for documentation updates
- `refactor/` - for code refactoring
- `test/` - for test improvements

### Making Changes

1. Make your changes in your branch
2. Add tests for new functionality
3. Update documentation if needed
4. Run tests locally (see Testing section)
5. Commit your changes with clear, descriptive messages

### Commit Message Guidelines

Follow conventional commit format:

```
<type>(<scope>): <subject>

<body>

<footer>
```

Types:
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation changes
- `test`: Adding or updating tests
- `refactor`: Code refactoring
- `perf`: Performance improvements
- `chore`: Maintenance tasks

Example:
```
feat(validator): add support for custom error messages

- Add custom_error parameter to ValidationErrorDetail
- Update error formatting in console reporter
- Add tests for custom error messages

Closes #123
```

## Testing

### Running Tests

Run the full test suite:

```bash
pytest tests/ -v
```

Run tests with coverage:

```bash
pytest tests/ --cov=src/pandera_unified_validator --cov-report=term-missing
```

Run specific test files:

```bash
pytest tests/test_core/test_validator.py -v
```

Run tests matching a pattern:

```bash
pytest -k "test_validation" -v
```

### Running Benchmarks

```bash
pytest tests/test_core/test_benchmarks.py --benchmark-only
```

### Running Property-Based Tests

```bash
pytest tests/test_core/test_hypothesis.py -v
```

### Test Requirements

- All new features must include tests
- Bug fixes must include regression tests
- Aim for >90% code coverage
- Tests must pass on Python 3.10, 3.11, and 3.12
- Include both unit tests and integration tests where appropriate

### Writing Tests

Example test structure:

```python
import pytest
from pandera_unified_validator import SchemaBuilder, UnifiedValidator

class TestNewFeature:
    """Tests for the new feature."""

    @pytest.fixture
    def sample_schema(self):
        """Fixture providing a sample schema."""
        return SchemaBuilder("test").add_column("id", int).build()

    def test_feature_basic_usage(self, sample_schema):
        """Test basic usage of the feature."""
        # Arrange
        validator = UnifiedValidator(sample_schema.to_validation_schema())
        data = {"id": 1}

        # Act
        result = validator.validate(data)

        # Assert
        assert result.is_valid is True

    def test_feature_edge_case(self, sample_schema):
        """Test edge case handling."""
        # Test implementation
        pass
```

## Code Style

### Python Code Style

We use the following tools to maintain code quality:

- **Black** - Code formatting
- **Ruff** - Linting
- **Mypy** - Type checking

### Running Code Quality Tools

Format code:

```bash
black src/ tests/
```

Check formatting:

```bash
black --check src/ tests/
```

Run linter:

```bash
ruff check src/ tests/
```

Fix linting issues automatically:

```bash
ruff check --fix src/ tests/
```

Run type checker:

```bash
mypy src/
```

Run all checks:

```bash
ruff check src/ && black --check src/ && mypy src/ && pytest
```

### Code Style Guidelines

1. **Type Hints**: All functions must have type hints
   ```python
   def validate_data(data: pd.DataFrame, schema: ValidationSchema) -> ValidationResult:
       ...
   ```

2. **Docstrings**: All public functions and classes must have docstrings
   ```python
   def process_data(data: pd.DataFrame) -> pd.DataFrame:
       """
       Process the input data.

       Args:
           data: Input DataFrame to process

       Returns:
           Processed DataFrame

       Raises:
           ValueError: If data is empty
       """
       ...
   ```

3. **Line Length**: Maximum 100 characters
4. **Imports**: Organized and sorted (handled by Black and Ruff)
5. **Error Handling**: Use specific exceptions, not bare `except:`

## Pull Request Process

### Before Submitting

1. Ensure all tests pass locally
2. Run code quality tools
3. Update documentation if needed
4. Add entry to CHANGELOG.md (if applicable)
5. Rebase your branch on the latest upstream main

### Submitting a Pull Request

1. Push your branch to your fork:
   ```bash
   git push origin feature/your-feature-name
   ```

2. Open a pull request on GitHub

3. Fill out the pull request template completely

4. Link any related issues

### Pull Request Review

- A maintainer will review your PR
- Address any feedback or requested changes
- Once approved, a maintainer will merge your PR

### PR Requirements

- All CI checks must pass
- Code coverage should not decrease
- At least one approval from a maintainer
- No unresolved conversations
- Commit history should be clean (squash if needed)

## Reporting Issues

### Bug Reports

When reporting bugs, please include:

1. **Description**: Clear description of the bug
2. **Steps to Reproduce**: Minimal code example
3. **Expected Behavior**: What you expected to happen
4. **Actual Behavior**: What actually happened
5. **Environment**:
   - Python version
   - pandera-unified-validator version
   - Operating system
   - Relevant package versions

Example:

```markdown
## Bug Description
Validation fails when using Polars DataFrame with null values

## Steps to Reproduce
\`\`\`python
import polars as pl
from pandera_unified_validator import SchemaBuilder, UnifiedValidator

schema = SchemaBuilder("test").add_column("value", int, nullable=True).build()
data = pl.DataFrame({"value": [1, None, 3]})
validator = UnifiedValidator(schema.to_validation_schema())
result = validator.validate(data)  # Fails here
\`\`\`

## Expected Behavior
Validation should pass with null values when nullable=True

## Actual Behavior
ValidationError: ...

## Environment
- Python 3.12
- pandera-unified-validator 0.1.0
- Polars 0.20.0
- macOS 14.0
```

### Feature Requests

When requesting features, please include:

1. **Use Case**: Describe the problem you're trying to solve
2. **Proposed Solution**: Your suggested implementation
3. **Alternatives**: Other solutions you've considered
4. **Examples**: Code examples of how the feature would be used

## Development Tips

### Running Examples

```bash
python examples/reporting_demo.py
python docs/examples/01_basic_validation.py
```

### Debugging Tests

Use pytest's debugging features:

```bash
# Drop into debugger on failure
pytest --pdb

# Verbose output
pytest -vv

# Show print statements
pytest -s
```

### Performance Profiling

```bash
# Run with profiling
pytest tests/ --profile

# Generate SVG profile
pytest tests/ --profile-svg
```

## Questions?

If you have questions:

1. Check the [documentation](docs/user_guide.md)
2. Search [existing issues](https://github.com/ianpinto/pandera-unified-validator/issues)
3. Open a new [discussion](https://github.com/ianpinto/pandera-unified-validator/discussions)

## License

By contributing to pandera-unified-validator, you agree that your contributions will be licensed under the MIT License.
