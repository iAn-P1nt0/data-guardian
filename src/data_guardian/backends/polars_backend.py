from __future__ import annotations

from typing import Any, Sequence

import pandas as pd
import polars as pl
from pandera import Check, Column, DataFrameSchema
from pandera.errors import SchemaError, SchemaErrors

from ..core.schema import ValidationSchema
from ..utils.reporting import ValidationReport
from . import ColumnValidationError, ValidationBackend


class PolarsBackend(ValidationBackend):
    """Validation backend for Polars dataframes."""

    name = "polars"

    def supports(self, data: object) -> bool:
        return isinstance(data, pl.DataFrame)

    def normalize(self, data: object) -> pl.DataFrame:
        if isinstance(data, pl.DataFrame):
            return data
        raise TypeError("Polars backend expects a polars.DataFrame")

    def validate(self, frame: pl.DataFrame, schema: ValidationSchema) -> ValidationReport:
        frame_report = schema.validate_polars(frame)
        if schema.record_model is not None:
            record_report = schema.validate_records(frame.to_dicts())
            frame_report = frame_report.merge(record_report)
        return frame_report.with_metadata(backend=self.name, rows=frame.height)

    def validate_column(
        self,
        df: pl.DataFrame,
        column: str,
        checks: Sequence[Check],
    ) -> list[ColumnValidationError]:
        if column not in df.columns:
            return [ColumnValidationError(column=column, message="Column missing", rows=tuple())]
        if not checks:
            return []

        pandas_frame = df.select(column).to_pandas()
        schema = DataFrameSchema({column: Column(pandas_frame[column].dtype, checks=list(checks))})
        try:
            schema.validate(pandas_frame, lazy=True)
        except SchemaErrors as exc:
            return self._errors_from_failure_cases(column, exc.failure_cases)
        except SchemaError as exc:  # pragma: no cover
            return [ColumnValidationError(column=column, message=str(exc), rows=tuple())]
        return []

    def get_column_dtype(self, df: pl.DataFrame, column: str) -> str:
        return str(df.schema[column]) if column in df.schema else "unknown"

    def filter_invalid_rows(
        self,
        df: pl.DataFrame,
        errors: Sequence[ColumnValidationError],
    ) -> tuple[pl.DataFrame, pl.DataFrame]:
        invalid_indices = self._collect_indices(errors)
        if not invalid_indices:
            empty = df.head(0)
            return df.clone(), empty

        invalid = df.take(invalid_indices)
        helper = df.with_row_count("__dg_row__")
        valid = helper.filter(~pl.col("__dg_row__").is_in(invalid_indices)).drop("__dg_row__")
        return valid, invalid

    def _errors_from_failure_cases(
        self,
        column: str,
        failure_cases: pd.DataFrame,
    ) -> list[ColumnValidationError]:
        errors: list[ColumnValidationError] = []
        for _, row in failure_cases.iterrows():
            index_value = row.get("index")
            indices: tuple[int, ...]
            if pd.notna(index_value):
                indices = (int(index_value),)
            else:
                indices = tuple()
            errors.append(
                ColumnValidationError(
                    column=column,
                    message=str(row.get("failure_case", "Validation failed")),
                    rows=indices,
                    check=str(row.get("check")) if row.get("check") else None,
                )
            )
        return errors

    def _collect_indices(self, errors: Sequence[ColumnValidationError]) -> list[int]:
        seen: set[int] = set()
        ordered: list[int] = []
        for error in errors:
            for idx in error.rows:
                if isinstance(idx, int) and idx not in seen:
                    seen.add(idx)
                    ordered.append(idx)
        return ordered
