from __future__ import annotations

from typing import Any, Mapping, Sequence

import pandas as pd
import pandera as pa
from pandera import Check, Column, DataFrameSchema
from pandera.errors import SchemaError, SchemaErrors

from ..core.schema import ValidationSchema
from ..utils.reporting import ValidationReport
from . import ColumnValidationError, ValidationBackend


class PandasBackend(ValidationBackend):
    """Validation backend powered by pandas + Pandera."""

    name = "pandas"

    def supports(self, data: object) -> bool:
        return isinstance(data, pd.DataFrame)

    def normalize(self, data: object) -> pd.DataFrame:
        if isinstance(data, pd.DataFrame):
            return data
        if isinstance(data, Mapping):
            return pd.DataFrame([data])
        if isinstance(data, Sequence) and all(isinstance(row, Mapping) for row in data):
            return pd.DataFrame(list(data))
        raise TypeError("Unsupported payload for pandas backend")

    def validate(self, frame: pd.DataFrame, schema: ValidationSchema) -> ValidationReport:
        frame_report = schema.validate_dataframe(frame)
        if schema.record_model is not None:
            record_report = schema.validate_records(frame.to_dict(orient="records"))
            frame_report = frame_report.merge(record_report)
        return frame_report.with_metadata(backend=self.name, rows=len(frame))

    def validate_column(
        self,
        df: pd.DataFrame,
        column: str,
        checks: Sequence[Check],
    ) -> list[ColumnValidationError]:
        if column not in df:
            return [ColumnValidationError(column=column, message="Column missing", rows=tuple())]
        if not checks:
            return []

        schema = DataFrameSchema({column: Column(df[column].dtype, checks=list(checks))})
        try:
            schema.validate(df[[column]], lazy=True)
        except SchemaErrors as exc:
            return self._errors_from_failure_cases(column, exc.failure_cases)
        except SchemaError as exc:  # pragma: no cover - defensive path for non-lazy errors
            return [ColumnValidationError(column=column, message=str(exc), rows=tuple())]
        return []

    def get_column_dtype(self, df: pd.DataFrame, column: str) -> str:
        if column not in df:
            return "unknown"
        return str(df[column].dtype)

    def filter_invalid_rows(
        self,
        df: pd.DataFrame,
        errors: Sequence[ColumnValidationError],
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        invalid_indices = self._collect_indices(errors)
        if not invalid_indices:
            empty = df.iloc[0:0]
            return df.copy(), empty
        invalid = df.loc[invalid_indices]
        valid = df.drop(index=invalid_indices)
        return valid, invalid

    def _errors_from_failure_cases(
        self,
        column: str,
        failure_cases: pd.DataFrame,
    ) -> list[ColumnValidationError]:
        errors: list[ColumnValidationError] = []
        for _, row in failure_cases.iterrows():
            index_value = row.get("index")
            indices: tuple[int | str, ...]
            if pd.notna(index_value):
                indices = (index_value,)
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

    def _collect_indices(self, errors: Sequence[ColumnValidationError]) -> list[int | str]:
        seen: set[int | str] = set()
        ordered: list[int | str] = []
        for error in errors:
            for idx in error.rows:
                if idx not in seen:
                    seen.add(idx)
                    ordered.append(idx)
        return ordered
