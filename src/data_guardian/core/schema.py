from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Mapping,
    Sequence,
    Type,
    TypeVar,
    Union,
)

import pandas as pd
import pandera as pa
from pandera import Check, Column, DataFrameModel, DataFrameSchema
from pydantic import BaseModel, Field, ValidationError, create_model

from ..utils.reporting import ValidationReport

SchemaLike = pa.DataFrameSchema | type[DataFrameModel]

# Type aliases for validators
ColumnValidator = Callable[[pd.Series], pd.Series]
RowValidator = Callable[[pd.DataFrame], pd.Series]
CrossColumnValidator = Callable[[pd.DataFrame], pd.Series]


DTYPE_MAP: Dict[type | str, Any] = {
    int: pa.Int64,
    float: pa.Float64,
    str: pa.String,
    bool: pa.Bool,
    "int": pa.Int64,
    "int64": pa.Int64,
    "float": pa.Float64,
    "float64": pa.Float64,
    "str": pa.String,
    "string": pa.String,
    "bool": pa.Bool,
    "datetime64[ns]": pa.DateTime,
    "datetime": pa.DateTime,
    "object": pa.Object,
}


@dataclass
class ColumnSpec:
    """Specification for a single column with merged constraints."""

    name: str
    dtype: type | str
    nullable: bool = True
    unique: bool = False
    ge: float | int | None = None
    le: float | int | None = None
    gt: float | int | None = None
    lt: float | int | None = None
    pattern: str | None = None
    isin: Sequence[Any] | None = None
    custom_checks: List[Check] = field(default_factory=list)
    description: str | None = None

    def to_pandera_column(self) -> Column:
        """Convert to Pandera Column with all checks."""
        checks: List[Check] = list(self.custom_checks)

        if self.ge is not None:
            checks.append(Check.greater_than_or_equal_to(self.ge))
        if self.le is not None:
            checks.append(Check.less_than_or_equal_to(self.le))
        if self.gt is not None:
            checks.append(Check.greater_than(self.gt))
        if self.lt is not None:
            checks.append(Check.less_than(self.lt))
        if self.pattern is not None:
            checks.append(Check.str_matches(self.pattern))
        if self.isin is not None:
            checks.append(Check.isin(list(self.isin)))

        pandera_dtype = DTYPE_MAP.get(self.dtype, self.dtype)
        return Column(
            dtype=pandera_dtype,
            nullable=self.nullable,
            unique=self.unique,
            checks=checks if checks else None,
            description=self.description,
        )

    def to_pydantic_field_info(self) -> tuple[type, Any]:
        """Convert to Pydantic field type and FieldInfo."""
        python_type = self.dtype if isinstance(self.dtype, type) else str
        if self.nullable:
            python_type = python_type | None  # type: ignore[assignment]

        field_kwargs: Dict[str, Any] = {}
        if self.ge is not None:
            field_kwargs["ge"] = self.ge
        if self.le is not None:
            field_kwargs["le"] = self.le
        if self.gt is not None:
            field_kwargs["gt"] = self.gt
        if self.lt is not None:
            field_kwargs["lt"] = self.lt
        if self.pattern is not None:
            field_kwargs["pattern"] = self.pattern
        if self.description is not None:
            field_kwargs["description"] = self.description

        default = None if self.nullable else ...
        return (python_type, Field(default=default, **field_kwargs))

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dictionary for JSON/YAML export."""
        result: Dict[str, Any] = {
            "name": self.name,
            "dtype": self.dtype if isinstance(self.dtype, str) else self.dtype.__name__,
            "nullable": self.nullable,
            "unique": self.unique,
        }
        if self.ge is not None:
            result["ge"] = self.ge
        if self.le is not None:
            result["le"] = self.le
        if self.gt is not None:
            result["gt"] = self.gt
        if self.lt is not None:
            result["lt"] = self.lt
        if self.pattern is not None:
            result["pattern"] = self.pattern
        if self.isin is not None:
            result["isin"] = list(self.isin)
        if self.description is not None:
            result["description"] = self.description
        return result


@dataclass
class CustomValidator:
    """Wrapper for custom validation functions."""

    name: str
    validator: ColumnValidator | RowValidator | CrossColumnValidator
    columns: List[str] | None = None
    error_message: str | None = None

    def to_pandera_check(self) -> Check:
        """Convert to Pandera Check."""
        return Check(
            self.validator,
            name=self.name,
            error=self.error_message or f"Custom check '{self.name}' failed",
        )


@dataclass
class CrossColumnConstraint:
    """Represents a cross-column validation rule."""

    name: str
    columns: List[str]
    validator: CrossColumnValidator
    error_message: str | None = None


@dataclass
class ConditionalConstraint:
    """Represents a conditional validation rule."""

    name: str
    condition_column: str
    target_column: str
    validator: ColumnValidator
    condition: Callable[[Any], bool] | None = None
    error_message: str | None = None


@dataclass
class UnifiedSchema:
    """Schema container with column definitions, validators, and conversion utilities."""

    name: str
    columns: Dict[str, ColumnSpec] = field(default_factory=dict)
    custom_validators: List[CustomValidator] = field(default_factory=list)
    cross_column_constraints: List[CrossColumnConstraint] = field(default_factory=list)
    conditional_constraints: List[ConditionalConstraint] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_pandera(self) -> DataFrameSchema:
        """Convert to Pandera DataFrameSchema."""
        pandera_columns: Dict[str, Column] = {}
        for col_name, col_spec in self.columns.items():
            pandera_columns[col_name] = col_spec.to_pandera_column()

        checks: List[Check] = []
        for custom in self.custom_validators:
            checks.append(custom.to_pandera_check())

        for cross in self.cross_column_constraints:
            checks.append(
                Check(
                    cross.validator,
                    name=cross.name,
                    error=cross.error_message or f"Cross-column check '{cross.name}' failed",
                )
            )

        return DataFrameSchema(columns=pandera_columns, checks=checks if checks else None)

    def to_pydantic(self, model_name: str | None = None) -> Type[BaseModel]:
        """Convert to dynamically created Pydantic model."""
        field_definitions: Dict[str, Any] = {}
        for col_name, col_spec in self.columns.items():
            field_definitions[col_name] = col_spec.to_pydantic_field_info()

        name = model_name or f"{self.name}Model"
        return create_model(name, **field_definitions)

    def to_validation_schema(self) -> "ValidationSchema":
        """Convert to ValidationSchema for use with validators."""
        return ValidationSchema(
            name=self.name,
            record_model=self.to_pydantic(),
            dataframe_schema=self.to_pandera(),
        )

    def to_json(self, indent: int = 2) -> str:
        """Serialize schema to JSON string."""
        data = {
            "name": self.name,
            "columns": {name: spec.to_dict() for name, spec in self.columns.items()},
            "metadata": self.metadata,
        }
        return json.dumps(data, indent=indent)

    def to_dict(self) -> Dict[str, Any]:
        """Serialize schema to dictionary."""
        return {
            "name": self.name,
            "columns": {name: spec.to_dict() for name, spec in self.columns.items()},
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "UnifiedSchema":
        """Deserialize schema from dictionary."""
        columns: Dict[str, ColumnSpec] = {}
        for col_name, col_data in data.get("columns", {}).items():
            dtype_str = col_data.get("dtype", "str")
            dtype: type | str = dtype_str
            if dtype_str in ("int", "int64"):
                dtype = int
            elif dtype_str in ("float", "float64"):
                dtype = float
            elif dtype_str in ("str", "string"):
                dtype = str
            elif dtype_str == "bool":
                dtype = bool

            columns[col_name] = ColumnSpec(
                name=col_name,
                dtype=dtype,
                nullable=col_data.get("nullable", True),
                unique=col_data.get("unique", False),
                ge=col_data.get("ge"),
                le=col_data.get("le"),
                gt=col_data.get("gt"),
                lt=col_data.get("lt"),
                pattern=col_data.get("pattern"),
                isin=col_data.get("isin"),
                description=col_data.get("description"),
            )

        return cls(
            name=data.get("name", "unnamed"),
            columns=columns,
            metadata=data.get("metadata", {}),
        )

    @classmethod
    def from_json(cls, json_str: str) -> "UnifiedSchema":
        """Deserialize schema from JSON string."""
        data = json.loads(json_str)
        return cls.from_dict(data)


class SchemaBuilder:
    """Fluent API for building UnifiedSchema instances."""

    def __init__(self, name: str = "schema") -> None:
        self._name = name
        self._columns: Dict[str, ColumnSpec] = {}
        self._custom_validators: List[CustomValidator] = []
        self._cross_column_constraints: List[CrossColumnConstraint] = []
        self._conditional_constraints: List[ConditionalConstraint] = []
        self._metadata: Dict[str, Any] = {}

    def add_column(
        self,
        name: str,
        dtype: type | str,
        *,
        nullable: bool = True,
        unique: bool = False,
        ge: float | int | None = None,
        le: float | int | None = None,
        gt: float | int | None = None,
        lt: float | int | None = None,
        pattern: str | None = None,
        isin: Sequence[Any] | None = None,
        checks: List[Check] | None = None,
        description: str | None = None,
    ) -> "SchemaBuilder":
        """Add a column with constraints."""
        self._columns[name] = ColumnSpec(
            name=name,
            dtype=dtype,
            nullable=nullable,
            unique=unique,
            ge=ge,
            le=le,
            gt=gt,
            lt=lt,
            pattern=pattern,
            isin=isin,
            custom_checks=checks or [],
            description=description,
        )
        return self

    def add_custom_validator(
        self,
        name: str,
        validator: ColumnValidator | RowValidator,
        *,
        columns: List[str] | None = None,
        error_message: str | None = None,
    ) -> "SchemaBuilder":
        """Add a custom validation function."""
        self._custom_validators.append(
            CustomValidator(
                name=name,
                validator=validator,
                columns=columns,
                error_message=error_message,
            )
        )
        return self

    def add_cross_column_constraint(
        self,
        name: str,
        columns: List[str],
        validator: CrossColumnValidator,
        *,
        error_message: str | None = None,
    ) -> "SchemaBuilder":
        """Add a cross-column validation rule (e.g., end_date > start_date)."""
        self._cross_column_constraints.append(
            CrossColumnConstraint(
                name=name,
                columns=columns,
                validator=validator,
                error_message=error_message,
            )
        )
        return self

    def add_conditional_constraint(
        self,
        name: str,
        condition_column: str,
        target_column: str,
        validator: ColumnValidator,
        *,
        condition: Callable[[Any], bool] | None = None,
        error_message: str | None = None,
    ) -> "SchemaBuilder":
        """Add conditional validation (if column X exists/matches, validate Y)."""
        self._conditional_constraints.append(
            ConditionalConstraint(
                name=name,
                condition_column=condition_column,
                target_column=target_column,
                validator=validator,
                condition=condition,
                error_message=error_message,
            )
        )
        return self

    def with_metadata(self, **kwargs: Any) -> "SchemaBuilder":
        """Attach arbitrary metadata to the schema."""
        self._metadata.update(kwargs)
        return self

    def build(self) -> UnifiedSchema:
        """Build the final UnifiedSchema."""
        return UnifiedSchema(
            name=self._name,
            columns=self._columns,
            custom_validators=self._custom_validators,
            cross_column_constraints=self._cross_column_constraints,
            conditional_constraints=self._conditional_constraints,
            metadata=self._metadata,
        )


class SchemaConverter:
    """Utilities for converting between schema formats."""

    @staticmethod
    def from_pydantic(model: Type[BaseModel], name: str | None = None) -> UnifiedSchema:
        """Convert a Pydantic model to UnifiedSchema."""
        columns: Dict[str, ColumnSpec] = {}
        schema_name = name or model.__name__

        for field_name, field_info in model.model_fields.items():
            annotation = field_info.annotation
            python_type: type | str = str
            nullable = False

            if annotation is not None:
                origin = getattr(annotation, "__origin__", None)
                if origin is Union:
                    args = getattr(annotation, "__args__", ())
                    non_none = [a for a in args if a is not type(None)]
                    if non_none:
                        python_type = non_none[0]
                    nullable = type(None) in args
                else:
                    python_type = annotation
                    nullable = field_info.default is None

            constraints: Dict[str, Any] = {}
            if field_info.metadata:
                for meta in field_info.metadata:
                    if hasattr(meta, "ge"):
                        constraints["ge"] = meta.ge
                    if hasattr(meta, "le"):
                        constraints["le"] = meta.le
                    if hasattr(meta, "gt"):
                        constraints["gt"] = meta.gt
                    if hasattr(meta, "lt"):
                        constraints["lt"] = meta.lt
                    if hasattr(meta, "pattern"):
                        constraints["pattern"] = meta.pattern

            columns[field_name] = ColumnSpec(
                name=field_name,
                dtype=python_type,
                nullable=nullable,
                description=field_info.description,
                **constraints,
            )

        return UnifiedSchema(name=schema_name, columns=columns)

    @staticmethod
    def from_pandera(
        schema: DataFrameSchema | Type[DataFrameModel],
        name: str | None = None,
    ) -> UnifiedSchema:
        """Convert a Pandera schema to UnifiedSchema."""
        if isinstance(schema, type) and issubclass(schema, DataFrameModel):
            df_schema = schema.to_schema()
            schema_name = name or schema.__name__
        else:
            df_schema = schema
            schema_name = name or "pandera_schema"

        columns: Dict[str, ColumnSpec] = {}
        for col_name, col in df_schema.columns.items():
            python_type: type | str = str
            if col.dtype is not None:
                dtype_str = str(col.dtype)
                if "int" in dtype_str.lower():
                    python_type = int
                elif "float" in dtype_str.lower():
                    python_type = float
                elif "bool" in dtype_str.lower():
                    python_type = bool
                elif "datetime" in dtype_str.lower():
                    python_type = "datetime64[ns]"
                else:
                    python_type = str

            columns[col_name] = ColumnSpec(
                name=col_name,
                dtype=python_type,
                nullable=col.nullable,
                unique=col.unique,
                description=col.description,
            )

        return UnifiedSchema(name=schema_name, columns=columns)

    @staticmethod
    def infer_from_dataframe(
        df: pd.DataFrame,
        name: str = "inferred_schema",
        *,
        infer_constraints: bool = False,
    ) -> UnifiedSchema:
        """Infer schema from a pandas DataFrame."""
        columns: Dict[str, ColumnSpec] = {}

        for col_name in df.columns:
            series = df[col_name]
            dtype_str = str(series.dtype)

            python_type: type | str
            if "int" in dtype_str:
                python_type = int
            elif "float" in dtype_str:
                python_type = float
            elif "bool" in dtype_str:
                python_type = bool
            elif "datetime" in dtype_str:
                python_type = "datetime64[ns]"
            else:
                python_type = str

            nullable = series.isna().any()

            constraints: Dict[str, Any] = {}
            if infer_constraints and python_type in (int, float):
                if not series.isna().all():
                    constraints["ge"] = float(series.min())
                    constraints["le"] = float(series.max())

            if infer_constraints and python_type == str:
                unique_count = series.nunique()
                total_count = len(series)
                if unique_count <= min(10, max(1, int(total_count * 0.5))):
                    constraints["isin"] = series.dropna().unique().tolist()

            columns[col_name] = ColumnSpec(
                name=col_name,
                dtype=python_type,
                nullable=nullable,
                unique=series.is_unique,
                **constraints,
            )

        return UnifiedSchema(name=name, columns=columns)


@dataclass(frozen=True)
class ValidationSchema:
    """Couples Pydantic models with Pandera dataframe schemas."""

    name: str
    record_model: type[BaseModel] | None = None
    dataframe_schema: SchemaLike | None = None

    def validate_records(self, rows: Iterable[Mapping[str, Any]]) -> ValidationReport:
        """Validate iterable of dictionary-like rows via the configured Pydantic model."""

        if self.record_model is None:
            return ValidationReport.ok(message="Record validation skipped", stage="records")

        errors: list[str] = []
        count = 0
        for count, row in enumerate(rows, start=1):
            try:
                self.record_model.model_validate(row)
            except ValidationError as exc:
                errors.append(str(exc))

        if errors:
            return ValidationReport(False, tuple(errors), (), {"stage": "records", "count": count})

        return ValidationReport.ok(message="Record validation passed", stage="records", count=count)

    def validate_dataframe(self, frame: Any) -> ValidationReport:
        """Validate a pandas-compatible dataframe via Pandera."""

        schema = self._materialize_schema()
        if schema is None:
            return ValidationReport.ok(message="Dataframe validation skipped", stage="dataframe")

        try:
            schema.validate(frame)
        except pa.errors.SchemaError as exc:
            return ValidationReport.from_exception(exc, stage="dataframe")

        size = getattr(frame, "shape", (None, None))[0]
        return ValidationReport.ok(message="Dataframe validation passed", stage="dataframe", rows=size)

    def validate_polars(self, frame: Any) -> ValidationReport:
        """Validate a Polars dataframe by converting it to pandas for Pandera inspection."""

        schema = self._materialize_schema()
        if schema is None:
            return ValidationReport.ok(message="Dataframe validation skipped", stage="dataframe")

        try:
            pandas_frame = frame.to_pandas()
        except AttributeError as exc:  # pragma: no cover - defensive
            return ValidationReport.from_exception(exc, stage="polars-conversion")

        return self.validate_dataframe(pandas_frame).with_metadata(source_backend="polars")

    def _materialize_schema(self) -> pa.DataFrameSchema | None:
        schema = self.dataframe_schema
        if schema is None:
            return None
        if isinstance(schema, pa.DataFrameSchema):
            return schema
        if isinstance(schema, type) and issubclass(schema, DataFrameModel):
            return schema.to_schema()
        raise TypeError("Unsupported schema type provided to ValidationSchema")
