"""Public interface for the pandera_unified_validator package."""

from .core.schema import (
    ColumnSpec,
    SchemaBuilder,
    SchemaConverter,
    UnifiedSchema,
    ValidationSchema,
)
from .core.streaming import (
    StreamingResult,
    StreamingValidator,
    ValidationMetrics,
    validate_csv_streaming,
    validate_csv_streaming_sync,
)
from .core.validator import (
    AutoFixSuggestion,
    DataGuardianValidator,
    UnifiedValidator,
    ValidationErrorDetail,
    ValidationResult,
)
from .backends.pandas_backend import PandasBackend
from .backends.polars_backend import PolarsBackend
from .profiling import (
    ComparisonReport,
    DataProfiler,
    HistogramSummary,
    ProfileReport,
    QualityScore,
    infer_constraints_from_profile,
)
from .utils.logging_config import configure_logging, get_logger
from .utils.reporting import (
    MetricsExporter,
    ValidationReport,
    ValidationReporter,
)

__all__ = [
    "AutoFixSuggestion",
    "ColumnSpec",
    "DataGuardianValidator",
    "PandasBackend",
    "PolarsBackend",
    "ComparisonReport",
    "DataProfiler",
    "HistogramSummary",
    "ProfileReport",
    "SchemaBuilder",
    "SchemaConverter",
    "StreamingResult",
    "StreamingValidator",
    "UnifiedSchema",
    "UnifiedValidator",
    "ValidationMetrics",
    "ValidationSchema",
    "ValidationErrorDetail",
    "ValidationResult",
    "ValidationReport",
    "ValidationReporter",
    "MetricsExporter",
    "QualityScore",
    "infer_constraints_from_profile",
    "validate_csv_streaming",
    "validate_csv_streaming_sync",
    "configure_logging",
    "get_logger",
]

__version__ = "0.1.0"
