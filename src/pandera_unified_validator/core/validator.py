from __future__ import annotations

from dataclasses import dataclass, field
from itertools import islice
import re
from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    Iterator,
    Mapping,
    Optional,
    Sequence,
    Type,
    TypeVar,
    Union,
)

from pandera import DataFrameModel
import pandas as pd
import polars as pl
from pydantic import BaseModel
from rich.console import Console
from rich.table import Table

from .schema import ValidationSchema
from ..backends import BackendFactory, ValidationBackend
from ..utils.reporting import ValidationReport
SchemaT = TypeVar("SchemaT", Type[BaseModel], Type[DataFrameModel], ValidationSchema)
DataLike = Union[pd.DataFrame, pl.DataFrame, Mapping[str, Any], Sequence[Mapping[str, Any]]]
FixCallable = Callable[[pd.DataFrame], pd.DataFrame]


@dataclass(frozen=True)
class ValidationErrorDetail:
    """Structured error detail with localization info."""

    message: str
    row: int | None = None
    column: str | None = None
    context: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class AutoFixSuggestion:
    """Represents a possible automatic remediation for an error."""

    description: str
    column: str | None = None
    fixer: FixCallable | None = None

    def apply(self, frame: pd.DataFrame) -> pd.DataFrame:
        if self.fixer is None:
            return frame
        return self.fixer(frame)


@dataclass
class ValidationResult:
    """Validation outcome returned by UnifiedValidator."""

    is_valid: bool
    errors: list[ValidationErrorDetail] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    suggestions: list[AutoFixSuggestion] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def raise_for_errors(self) -> None:
        if not self.is_valid:
            raise ValidationFailedError(self)


class ValidationException(Exception):
    """Base error for validation failures."""


class BackendNotAvailableError(ValidationException):
    """Raised when no backend can handle the provided payload."""


class ValidationFailedError(ValidationException):
    """Raised when validation fails in eager mode."""

    def __init__(self, result: ValidationResult):
        super().__init__("Validation failed")
        self.result = result


class UnifiedValidator(Generic[SchemaT]):
    """High-level validator capable of handling multiple schema types and payloads."""

    def __init__(
        self,
        schema: SchemaT,
        *,
        lazy: bool = False,
        auto_fix: bool = False,
        backend: str | None = "pandas",
        console: Console | None = None,
    ) -> None:
        self._schema = self._coerce_schema(schema)
        self.lazy = lazy
        self.auto_fix = auto_fix
        self.default_backend = backend
        self._console = console
        self._backends: Dict[str, ValidationBackend] = {}
        self.register_backend(BackendFactory.get_backend_by_name("pandas"), override=True)
        self.register_backend(BackendFactory.get_backend_by_name("polars"), override=True)

    def register_backend(self, backend: ValidationBackend, *, override: bool = False) -> None:
        if not override and backend.name in self._backends:
            raise ValueError(f"Backend '{backend.name}' already registered for UnifiedValidator")
        self._backends[backend.name] = backend

    def validate(
        self,
        data: DataLike,
        *,
        backend: str | None = None,
        console: Console | None = None,
    ) -> ValidationResult:
        prepared = self._prepare_payload(data)
        backend_name = backend or self.default_backend
        backend_impl = self._resolve_backend(prepared, backend_name)
        normalized = backend_impl.normalize(prepared)
        report = backend_impl.validate(normalized, self._schema)
        result = self._result_from_report(report)
        if self.auto_fix:
            result.suggestions.extend(self._suggest_fixes(result))

        display_console = console or self._console
        if display_console is not None:
            self._render_console(display_console, result)

        if not result.is_valid and not self.lazy:
            raise ValidationFailedError(result)

        return result

    def validate_streaming(
        self,
        data_stream: Iterator[Mapping[str, Any]],
        *,
        chunk_size: int = 1000,
    ) -> Iterator[ValidationResult]:
        while True:
            chunk = list(islice(data_stream, chunk_size))
            if not chunk:
                break
            yield self.validate(chunk)

    def apply_fixes(self, data: pd.DataFrame, result: ValidationResult) -> pd.DataFrame:
        frame = data.copy()
        for suggestion in result.suggestions:
            frame = suggestion.apply(frame)
        return frame

    def _coerce_schema(self, schema: SchemaT) -> ValidationSchema:
        if isinstance(schema, ValidationSchema):
            return schema
        if isinstance(schema, type) and issubclass(schema, BaseModel):
            return ValidationSchema(name=schema.__name__, record_model=schema)
        if isinstance(schema, type) and issubclass(schema, DataFrameModel):
            materialized = schema.to_schema()
            return ValidationSchema(name=schema.__name__, dataframe_schema=materialized)
        raise TypeError("Unsupported schema type provided to UnifiedValidator")

    def _prepare_payload(self, data: DataLike | pl.DataFrame) -> object:
        if isinstance(data, (pd.DataFrame, pl.DataFrame)):
            return data
        if isinstance(data, Mapping):
            return pd.DataFrame([data])
        if isinstance(data, Sequence) and all(isinstance(row, Mapping) for row in data):
            return pd.DataFrame(list(data))
        raise TypeError("Unsupported data payload provided to validate()")

    def _resolve_backend(self, data: object, name: str | None) -> ValidationBackend:
        if name is not None:
            backend = self._backends.get(name)
            if backend is not None:
                return backend
            try:
                backend = BackendFactory.get_backend_by_name(name)
            except ValueError as exc:  # pragma: no cover - defensive
                raise BackendNotAvailableError(f"Backend '{name}' is not registered") from exc
            self.register_backend(backend, override=True)
            return backend

        for backend in self._backends.values():
            if backend.supports(data):
                return backend

        try:
            backend = BackendFactory.get_backend(data)
        except ValueError as exc:
            raise BackendNotAvailableError(
                "No compatible backend found. Registered backends: " + ", ".join(self._backends.keys())
            ) from exc
        self.register_backend(backend, override=True)
        return backend

    def _result_from_report(self, report: ValidationReport) -> ValidationResult:
        errors = [ValidationErrorDetail(message=message, context=dict(report.metadata)) for message in report.errors]
        warnings = list(report.warnings)
        metadata = dict(report.metadata)
        return ValidationResult(report.is_valid, errors, warnings, [], metadata)

    def _suggest_fixes(self, result: ValidationResult) -> list[AutoFixSuggestion]:
        suggestions: list[AutoFixSuggestion] = []
        for error in result.errors:
            column = self._infer_column(error)
            lowered = error.message.lower()
            missing_column_issue = ("missing" in lowered and "column" in lowered) or "field required" in lowered
            if missing_column_issue and column:
                suggestions.append(
                    AutoFixSuggestion(
                        description=f"Add missing column '{column}' with null values",
                        column=column,
                        fixer=self._build_missing_column_fixer(column),
                    )
                )
            elif "type" in lowered and column:
                suggestions.append(
                    AutoFixSuggestion(
                        description=f"Coerce column '{column}' to the expected dtype",
                        column=column,
                        fixer=self._build_type_coercion_fixer(column),
                    )
                )
        return suggestions

    def _infer_column(self, error: ValidationErrorDetail) -> str | None:
        if error.column is not None:
            return error.column
        if isinstance(error.context, Mapping):
            column_value = error.context.get("column")
            if isinstance(column_value, str):
                return column_value
        lines = [line.strip() for line in error.message.splitlines() if line.strip()]
        if len(lines) >= 2:
            field_token = lines[1]
            if field_token and " " not in field_token:
                return field_token.strip("'\"")
        match = re.search(r"['\"](?P<col>[A-Za-z0-9_]+)['\"]", error.message)
        if match:
            return match.group("col")
        return None

    def _build_missing_column_fixer(self, column: str) -> FixCallable:
        def fixer(frame: pd.DataFrame, *, col: str = column) -> pd.DataFrame:
            if col not in frame:
                frame[col] = pd.NA
            return frame

        return fixer

    def _build_type_coercion_fixer(self, column: str) -> FixCallable:
        def fixer(frame: pd.DataFrame, *, col: str = column) -> pd.DataFrame:
            if col in frame:
                frame[col] = pd.to_numeric(frame[col], errors="coerce")
            return frame

        return fixer

    def _render_console(self, console: Console, result: ValidationResult) -> None:
        table = Table(title="Validation Result", expand=True)
        table.add_column("Type")
        table.add_column("Message")
        if result.is_valid:
            table.add_row("status", "Validation passed")
        else:
            table.add_row("status", "Validation failed")
        for error in result.errors:
            location = ""
            if error.column is not None:
                location += f"column={error.column} "
            if error.row is not None:
                location += f"row={error.row}"
            table.add_row("error", f"{error.message} {location}".strip())
        for warning in result.warnings:
            table.add_row("warning", warning)
        for suggestion in result.suggestions:
            table.add_row("suggestion", suggestion.description)
        console.print(table)

class DataGuardianValidator:
    """Historical validator retained for backwards compatibility."""

    def __init__(self) -> None:
        self._backends: Dict[str, ValidationBackend] = {}

    def register_backend(self, backend: ValidationBackend, *, override: bool = False) -> None:
        if not override and backend.name in self._backends:
            raise ValueError(f"Backend '{backend.name}' already registered")
        self._backends[backend.name] = backend

    def available_backends(self) -> tuple[str, ...]:
        return tuple(self._backends.keys())

    def validate(
        self,
        data: object,
        schema: ValidationSchema,
        *,
        backend: Optional[str] = None,
    ) -> ValidationReport:
        selected = self._resolve_backend(data, backend)
        return selected.validate(data, schema)

    def _resolve_backend(self, data: object, name: Optional[str]) -> ValidationBackend:
        if name is not None:
            try:
                return self._backends[name]
            except KeyError as exc:
                raise ValueError(f"Backend '{name}' is not registered") from exc

        for candidate in self._backends.values():
            if candidate.supports(data):
                return candidate

        raise ValueError(
            "No compatible backend found. Registered backends: " + ", ".join(self.available_backends())
        )

