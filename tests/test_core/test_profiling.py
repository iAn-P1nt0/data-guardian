from __future__ import annotations

import pandas as pd

from pandera_unified_validator import (
    DataProfiler,
    ProfileReport,
    QualityScore,
    infer_constraints_from_profile,
)


def _build_sample_dataframe() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
            "score": [72.5, 88.0, 91.2, 85.3, 77.9],
            "category": ["A", "B", "A", "B", "C"],
        }
    )


class TestDataProfiler:
    def test_profile_generates_column_profiles(self) -> None:
        df = _build_sample_dataframe()
        profiler = DataProfiler()

        report = profiler.profile(df, title="Sample")

        assert isinstance(report, ProfileReport)
        assert set(report.column_profiles.keys()) == {"id", "name", "score", "category"}
        assert report.duplicate_rows == 0
        assert report.schema_suggestions
        assert report.quality_score is not None
        assert report.quality_score.overall > 0

    def test_suggest_schema_from_profile(self) -> None:
        df = _build_sample_dataframe()
        profiler = DataProfiler()

        schema = profiler.suggest_schema(df)

        assert "score" in schema.columns
        score_column = schema.columns["score"]
        assert score_column.dtype in (float, "float")
        assert score_column.nullable is False
        assert score_column.ge is not None
        assert score_column.le is not None

    def test_compare_profiles_highlights_drift(self) -> None:
        df_a = _build_sample_dataframe()
        df_b = df_a.copy()
        df_b["score"] = df_b["score"] + 50

        profiler = DataProfiler()
        comparison = profiler.compare_profiles(df_a, df_b)

        assert "score" in comparison.column_drift
        assert comparison.column_drift["score"] > 0
        assert comparison.summary["comparison_rows"] == len(df_b)

    def test_infer_constraints_from_profile(self) -> None:
        df = _build_sample_dataframe()
        profiler = DataProfiler()
        profile = profiler.profile(df)

        schema = infer_constraints_from_profile(profile)

        assert set(schema.columns.keys()) == set(df.columns)
        assert schema.columns["id"].nullable is False


class TestQualityScore:
    def test_to_dict_round_trip(self) -> None:
        score = QualityScore(
            completeness=0.95,
            validity=0.9,
            consistency=0.85,
            uniqueness=0.8,
            timeliness=0.75,
            overall=0.85,
        )

        result = score.to_dict()

        assert result["completeness"] == 0.95
        assert result["overall"] == 0.85
