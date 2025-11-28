from __future__ import annotations

import math
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from statistics import mean
from typing import Any, Dict, List

import numpy as np
import pandas as pd

from ..core.schema import SchemaBuilder, UnifiedSchema

try:  # pragma: no cover - optional dependency
    import ydata_profiling  # type: ignore
except ImportError:  # pragma: no cover - optional dependency
    ydata_profiling = None  # type: ignore

try:  # pragma: no cover - optional dependency
    import sweetviz  # type: ignore
except ImportError:  # pragma: no cover - optional dependency
    sweetviz = None  # type: ignore


@dataclass
class HistogramSummary:
    """Lightweight histogram storage for profile visualizations."""

    bins: List[float]
    counts: List[int]

    def to_dict(self) -> Dict[str, List[float | int]]:
        return {"bins": self.bins, "counts": self.counts}


@dataclass
class ColumnSuggestion:
    """Schema suggestion information with a confidence score."""

    column: str
    dtype: str
    constraints: Dict[str, Any] = field(default_factory=dict)
    confidence: float = 0.5

    def to_dict(self) -> Dict[str, Any]:
        return {
            "column": self.column,
            "dtype": self.dtype,
            "constraints": self.constraints,
            "confidence": self.confidence,
        }


@dataclass
class ColumnProfile:
    """Per-column profiling summary."""

    name: str
    dtype: str
    missing_count: int
    missing_pct: float
    stats: Dict[str, float]
    histogram: HistogramSummary | None = None
    categorical_values: List[Any] | None = None
    regex_pattern: str | None = None
    unique_ratio: float | None = None
    suggestion: ColumnSuggestion | None = None

    def to_dict(self) -> Dict[str, Any]:
        data: Dict[str, Any] = {
            "name": self.name,
            "dtype": self.dtype,
            "missing_count": self.missing_count,
            "missing_pct": self.missing_pct,
            "stats": self.stats,
            "unique_ratio": self.unique_ratio,
        }
        if self.histogram:
            data["histogram"] = self.histogram.to_dict()
        if self.categorical_values is not None:
            data["categorical_values"] = list(self.categorical_values)
        if self.regex_pattern:
            data["regex_pattern"] = self.regex_pattern
        if self.suggestion:
            data["suggestion"] = self.suggestion.to_dict()
        return data


@dataclass
class QualityScore:
    """Data quality scoring across core dimensions."""

    completeness: float
    validity: float
    consistency: float
    uniqueness: float
    timeliness: float
    overall: float

    def to_dict(self) -> Dict[str, float]:
        return {
            "completeness": self.completeness,
            "validity": self.validity,
            "consistency": self.consistency,
            "uniqueness": self.uniqueness,
            "timeliness": self.timeliness,
            "overall": self.overall,
        }


@dataclass
class ProfileReport:
    """Aggregated profile output shared across backends."""

    title: str
    column_profiles: Dict[str, ColumnProfile]
    correlations: Dict[str, Dict[str, float]]
    duplicate_rows: int
    total_rows: int
    schema_suggestions: List[ColumnSuggestion]
    backend: str
    quality_score: QualityScore | None = None
    raw_backend_report: Any | None = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "title": self.title,
            "column_profiles": {name: profile.to_dict() for name, profile in self.column_profiles.items()},
            "correlations": self.correlations,
            "duplicate_rows": self.duplicate_rows,
            "total_rows": self.total_rows,
            "schema_suggestions": [suggestion.to_dict() for suggestion in self.schema_suggestions],
            "backend": self.backend,
            "quality_score": self.quality_score.to_dict() if self.quality_score else None,
        }


@dataclass
class ComparisonReport:
    """Report describing differences between two datasets."""

    base_title: str
    target_title: str
    column_drift: Dict[str, float]
    summary: Dict[str, Any]
    quality_delta: Dict[str, float]
    notes: List[str]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "base_title": self.base_title,
            "target_title": self.target_title,
            "column_drift": self.column_drift,
            "summary": self.summary,
            "quality_delta": self.quality_delta,
            "notes": self.notes,
        }


class DataProfiler:
    """Profile dataframes using configurable profiling backends."""

    SUPPORTED_BACKENDS = {
        "pandas-profiling",
        "ydata-profiling",
        "sweetviz",
    }

    def __init__(self, backend: str = "pandas-profiling") -> None:
        backend_normalized = backend.lower()
        if backend_normalized not in self.SUPPORTED_BACKENDS:
            raise ValueError(
                f"Unsupported backend '{backend}'. Supported: {sorted(self.SUPPORTED_BACKENDS)}"
            )
        self.backend = backend_normalized

    def profile(self, df: pd.DataFrame, title: str = "Data Profile Report") -> ProfileReport:
        if not isinstance(df, pd.DataFrame):
            raise TypeError("profile expects a pandas DataFrame")

        backend_report, backend_used = self._run_backend(df, title)
        column_profiles = self._build_column_profiles(df)
        schema_suggestions = [cp.suggestion for cp in column_profiles.values() if cp.suggestion]
        correlation_df = df.corr(numeric_only=True)
        correlations = correlation_df.fillna(0).to_dict() if not correlation_df.empty else {}
        duplicate_rows = int(len(df) - len(df.drop_duplicates()))
        quality_score = self._compute_quality_score(df, column_profiles)

        return ProfileReport(
            title=title,
            column_profiles=column_profiles,
            correlations=correlations,
            duplicate_rows=duplicate_rows,
            total_rows=len(df),
            schema_suggestions=schema_suggestions,
            backend=backend_used,
            quality_score=quality_score,
            raw_backend_report=backend_report,
        )

    def suggest_schema(self, df: pd.DataFrame) -> UnifiedSchema:
        profile = self.profile(df, title="Schema Suggestion Profile")
        return infer_constraints_from_profile(profile)

    def compare_profiles(self, df1: pd.DataFrame, df2: pd.DataFrame) -> ComparisonReport:
        profile_a = self.profile(df1, title="Baseline Dataset")
        profile_b = self.profile(df2, title="Comparison Dataset")

        shared_columns = set(profile_a.column_profiles).intersection(profile_b.column_profiles)
        column_drift: Dict[str, float] = {}
        for column in shared_columns:
            mean_a = profile_a.column_profiles[column].stats.get("mean")
            mean_b = profile_b.column_profiles[column].stats.get("mean")
            if mean_a is not None and mean_b is not None:
                denominator = abs(mean_a) if mean_a != 0 else 1
                column_drift[column] = abs(mean_b - mean_a) / denominator

        quality_delta: Dict[str, float] = {}
        if profile_a.quality_score and profile_b.quality_score:
            base_scores = profile_a.quality_score.to_dict()
            target_scores = profile_b.quality_score.to_dict()
            for key in base_scores:
                quality_delta[key] = target_scores[key] - base_scores[key]

        notes: List[str] = []
        for column, drift in column_drift.items():
            if drift > 0.2:
                notes.append(f"Column '{column}' shows significant drift ({drift:.2f}).")

        summary = {
            "baseline_rows": profile_a.total_rows,
            "comparison_rows": profile_b.total_rows,
            "duplicate_delta": profile_b.duplicate_rows - profile_a.duplicate_rows,
        }

        return ComparisonReport(
            base_title=profile_a.title,
            target_title=profile_b.title,
            column_drift=column_drift,
            summary=summary,
            quality_delta=quality_delta,
            notes=notes,
        )

    # Helpers -----------------------------------------------------------------

    def _run_backend(self, df: pd.DataFrame, title: str) -> tuple[Any | None, str]:
        if self.backend in {"pandas-profiling", "ydata-profiling"} and ydata_profiling is not None:
            profile = ydata_profiling.ProfileReport(df, title=title, explorative=True)
            return profile, self.backend
        if self.backend == "sweetviz" and sweetviz is not None:
            report = sweetviz.analyze(df)
            return report, self.backend
        return None, "built-in"

    def _build_column_profiles(self, df: pd.DataFrame) -> Dict[str, ColumnProfile]:
        profiles: Dict[str, ColumnProfile] = {}
        for column in df.columns:
            series = df[column]
            dtype_label = self._normalize_dtype(series)
            missing_count = int(series.isna().sum())
            total = max(len(series), 1)
            missing_pct = missing_count / total
            stats = self._compute_statistics(series)
            histogram = self._build_histogram(series)
            categorical_values = self._infer_categorical_values(series)
            regex_pattern = self._infer_regex_pattern(series)
            unique_ratio = self._compute_unique_ratio(series)

            suggestion = self._build_column_suggestion(
                column,
                dtype_label,
                stats,
                missing_pct,
                categorical_values,
                regex_pattern,
                unique_ratio,
            )

            profiles[column] = ColumnProfile(
                name=column,
                dtype=dtype_label,
                missing_count=missing_count,
                missing_pct=missing_pct,
                stats=stats,
                histogram=histogram,
                categorical_values=categorical_values,
                regex_pattern=regex_pattern,
                unique_ratio=unique_ratio,
                suggestion=suggestion,
            )
        return profiles

    def _compute_statistics(self, series: pd.Series) -> Dict[str, float]:
        numeric_series = pd.to_numeric(series, errors="coerce")
        numeric_series = numeric_series.dropna()
        if numeric_series.empty:
            return {}
        stats: Dict[str, float] = {
            "mean": float(numeric_series.mean()),
            "median": float(numeric_series.median()),
            "std": float(numeric_series.std(ddof=0)) if len(numeric_series) > 1 else 0.0,
            "min": float(numeric_series.min()),
            "max": float(numeric_series.max()),
            "q1": float(numeric_series.quantile(0.25)),
            "q3": float(numeric_series.quantile(0.75)),
        }
        return stats

    def _build_histogram(self, series: pd.Series) -> HistogramSummary | None:
        numeric_series = pd.to_numeric(series, errors="coerce").dropna()
        if numeric_series.empty:
            return None
        counts, bin_edges = np.histogram(numeric_series, bins=min(10, len(numeric_series)))
        return HistogramSummary(bins=bin_edges.tolist(), counts=counts.tolist())

    def _infer_categorical_values(self, series: pd.Series, max_unique: int = 20) -> List[Any] | None:
        unique_values = series.dropna().unique()
        if 0 < len(unique_values) <= max_unique:
            return unique_values.tolist()
        return None

    def _infer_regex_pattern(self, series: pd.Series) -> str | None:
        if not pd.api.types.is_string_dtype(series):
            return None
        sample = series.dropna().astype(str)
        if sample.empty:
            return None
        if sample.map(str.isdigit).all():
            return r"^\d+$"
        if sample.map(lambda v: bool(re.fullmatch(r"[A-Za-z0-9_-]+", v))).all():
            return r"^[A-Za-z0-9_-]+$"
        if sample.map(lambda v: bool(re.fullmatch(r"[A-Za-z\s]+", v))).all():
            return r"^[A-Za-z\s]+$"
        return None

    def _compute_unique_ratio(self, series: pd.Series) -> float | None:
        non_null = series.dropna()
        if non_null.empty:
            return None
        return non_null.nunique() / len(non_null)

    def _build_column_suggestion(
        self,
        column: str,
        dtype_label: str,
        stats: Dict[str, float],
        missing_pct: float,
        categorical_values: List[Any] | None,
        regex_pattern: str | None,
        unique_ratio: float | None,
    ) -> ColumnSuggestion:
        constraints: Dict[str, Any] = {"nullable": missing_pct > 0}
        confidence = 0.6

        if stats:
            if "min" in stats:
                constraints["ge"] = stats["min"]
            if "max" in stats:
                constraints["le"] = stats["max"]
            confidence += 0.1

        if categorical_values is not None:
            constraints["isin"] = categorical_values
            confidence += 0.1

        if regex_pattern:
            constraints["pattern"] = regex_pattern
            confidence += 0.1

        if unique_ratio and unique_ratio > 0.98:
            constraints["unique"] = True
            confidence += 0.1

        return ColumnSuggestion(column=column, dtype=dtype_label, constraints=constraints, confidence=min(confidence, 0.99))

    def _compute_quality_score(
        self, df: pd.DataFrame, column_profiles: Dict[str, ColumnProfile]
    ) -> QualityScore:
        if df.empty:
            return QualityScore(1.0, 1.0, 1.0, 1.0, 1.0, 1.0)

        completeness_scores: List[float] = []
        consistency_scores: List[float] = []
        uniqueness_scores: List[float] = []
        validity_scores: List[float] = []

        for profile in column_profiles.values():
            completeness_scores.append(_clamp(1 - profile.missing_pct))
            uniqueness_scores.append(_clamp(profile.unique_ratio or 0.0))
            consistency_scores.append(1.0 if profile.regex_pattern else 0.8)

            stats = profile.stats
            if stats:
                iqr = stats.get("q3", 0) - stats.get("q1", 0)
                if iqr <= 0:
                    validity_scores.append(1.0)
                else:
                    series = pd.to_numeric(df[profile.name], errors="coerce")
                    lower = stats.get("q1", 0) - 1.5 * iqr
                    upper = stats.get("q3", 0) + 1.5 * iqr
                    mask = (series < lower) | (series > upper)
                    invalid_ratio = float(mask.mean(skipna=True))
                    if math.isnan(invalid_ratio):
                        invalid_ratio = 0.0
                    validity_scores.append(_clamp(1 - invalid_ratio))
            else:
                validity_scores.append(1.0)

        completeness = float(mean(completeness_scores)) if completeness_scores else 1.0
        validity = float(mean(validity_scores)) if validity_scores else 1.0
        consistency = float(mean(consistency_scores)) if consistency_scores else 1.0
        uniqueness = float(mean(uniqueness_scores)) if uniqueness_scores else 1.0
        timeliness = self._compute_timeliness_score(df)

        overall = float(
            0.25 * completeness
            + 0.2 * validity
            + 0.2 * consistency
            + 0.2 * uniqueness
            + 0.15 * timeliness
        )
        return QualityScore(
            completeness=completeness,
            validity=validity,
            consistency=consistency,
            uniqueness=uniqueness,
            timeliness=timeliness,
            overall=overall,
        )

    def _compute_timeliness_score(self, df: pd.DataFrame) -> float:
        datetime_columns = [col for col in df.columns if pd.api.types.is_datetime64_any_dtype(df[col])]
        if not datetime_columns:
            return 0.8
        latest_values: List[datetime] = []
        for column in datetime_columns:
            series = pd.to_datetime(df[column], errors="coerce")
            if series.notna().any():
                latest_values.append(series.max())
        if not latest_values:
            return 0.8
        now = datetime.now(timezone.utc)
        days = min(max((now - max(latest_values)).days, 0), 180)
        return _clamp(1 - days / 180)

    def _normalize_dtype(self, series: pd.Series) -> str:
        if pd.api.types.is_integer_dtype(series):
            return "integer"
        if pd.api.types.is_float_dtype(series):
            return "float"
        if pd.api.types.is_bool_dtype(series):
            return "boolean"
        if pd.api.types.is_datetime64_any_dtype(series):
            return "datetime"
        return "string"


def infer_constraints_from_profile(profile: ProfileReport) -> UnifiedSchema:
    builder = SchemaBuilder(profile.title)
    for name, col_profile in profile.column_profiles.items():
        python_type = _dtype_to_python(col_profile.dtype)
        suggestion = col_profile.suggestion
        constraints = suggestion.constraints if suggestion else {}

        builder.add_column(
            name,
            python_type,
            nullable=constraints.get("nullable", True),
            unique=constraints.get("unique", False),
            ge=constraints.get("ge"),
            le=constraints.get("le"),
            pattern=constraints.get("pattern"),
            isin=constraints.get("isin"),
        )

    builder.with_metadata(profiling_backend=profile.backend)
    return builder.build()


def _dtype_to_python(dtype_label: str) -> type | str:
    mapping = {
        "integer": int,
        "float": float,
        "numeric": float,
        "boolean": bool,
        "datetime": "datetime64[ns]",
        "string": str,
    }
    return mapping.get(dtype_label, str)


def _clamp(value: float, lower: float = 0.0, upper: float = 1.0) -> float:
    return max(lower, min(upper, value))
