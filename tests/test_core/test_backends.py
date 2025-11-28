from __future__ import annotations

import time

import pandas as pd
import polars as pl
from pandera import Check

from pandera_unified_validator.backends import (
    BackendFactory,
    ColumnValidationError,
    PandasBackend,
    PolarsBackend,
)


def test_backend_factory_detects_pandas_and_polars() -> None:
    pandas_backend = BackendFactory.get_backend(pd.DataFrame({"a": [1, 2]}))
    assert isinstance(pandas_backend, PandasBackend)

    polars_backend = BackendFactory.get_backend(pl.DataFrame({"a": [1, 2]}))
    assert isinstance(polars_backend, PolarsBackend)


class TestPandasBackend:
    def test_validate_column_and_filter_rows(self) -> None:
        df = pd.DataFrame({"value": [10, -5, 20]})
        backend = PandasBackend()

        errors = backend.validate_column(df, "value", [Check.greater_than_or_equal_to(0)])
        assert errors
        assert errors[0].column == "value"

        valid, invalid = backend.filter_invalid_rows(df, errors)
        assert len(valid) == 2
        assert len(invalid) == 1
        assert invalid.iloc[0]["value"] == -5

    def test_filter_invalid_rows_performance(self) -> None:
        rows = 10_000
        df = pd.DataFrame({"value": list(range(rows))})
        backend = PandasBackend()
        errors = [ColumnValidationError(column="value", message="fail", rows=(i,)) for i in range(0, rows, 2)]

        start = time.perf_counter()
        valid, invalid = backend.filter_invalid_rows(df, errors)
        duration = time.perf_counter() - start

        assert len(valid) == rows // 2
        assert len(invalid) == rows // 2
        assert duration < 1.0  # basic performance guard


class TestPolarsBackend:
    def test_validate_column_and_filter_rows(self) -> None:
        df = pl.DataFrame({"value": [1, -2, 3]})
        backend = PolarsBackend()

        errors = backend.validate_column(df, "value", [Check.greater_than_or_equal_to(0)])
        assert errors
        assert errors[0].column == "value"

        valid, invalid = backend.filter_invalid_rows(df, errors)
        assert valid.height == 2
        assert invalid.height == 1
        assert invalid["value"].to_list() == [-2]

    def test_get_column_dtype(self) -> None:
        df = pl.DataFrame({"name": ["a", "b"]})
        backend = PolarsBackend()

        dtype = backend.get_column_dtype(df, "name")
        assert "Utf8" in dtype or "String" in dtype
