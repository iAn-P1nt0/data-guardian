"""Backend implementations and abstractions for dataframe libraries."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol, Sequence, runtime_checkable

import pandas as pd
import polars as pl
from pandera import Check

from ..core.schema import ValidationSchema
from ..utils.reporting import ValidationReport


@dataclass(frozen=True)
class ColumnValidationError:
	"""Represents a validation failure for a specific dataframe column."""

	column: str
	message: str
	rows: Sequence[int | str] = field(default_factory=tuple)
	check: str | None = None

	def to_dict(self) -> dict[str, Any]:
		return {
			"column": self.column,
			"message": self.message,
			"rows": list(self.rows),
			"check": self.check,
		}


@runtime_checkable
class ValidationBackend(Protocol):
	"""Protocol describing dataframe validation backends."""

	name: str

	def supports(self, data: object) -> bool:
		"""Return ``True`` when ``data`` can be processed by this backend."""

	def normalize(self, data: object) -> Any:
		"""Convert user-provided input into the backend's dataframe type."""

	def validate(self, frame: Any, schema: ValidationSchema) -> ValidationReport:
		"""Run full-schema validation for ``frame`` using ``schema``."""

	def validate_column(self, df: Any, column: str, checks: Sequence[Check]) -> list[ColumnValidationError]:
		"""Validate a single column against ``checks`` and return errors."""

	def get_column_dtype(self, df: Any, column: str) -> str:
		"""Return a user-friendly dtype label for ``column``."""

	def filter_invalid_rows(self, df: Any, errors: Sequence[ColumnValidationError]) -> tuple[Any, Any]:
		"""Split ``df`` into (valid, invalid) frames using row indices from ``errors``."""


class BackendFactory:
	"""Factory for retrieving validation backends based on input data."""

	_PANDAS_TYPES = (pd.DataFrame,)
	_POLARS_TYPES = (pl.DataFrame,)

	@staticmethod
	def get_backend(data: Any) -> ValidationBackend:
		"""Auto-detect a backend based on the incoming dataframe type."""

		from .pandas_backend import PandasBackend  # Local import to avoid cycles
		from .polars_backend import PolarsBackend

		if isinstance(data, BackendFactory._PANDAS_TYPES):
			return PandasBackend()
		if isinstance(data, BackendFactory._POLARS_TYPES):
			return PolarsBackend()
		raise ValueError(f"Unsupported data type: {type(data)!r}")

	@staticmethod
	def get_backend_by_name(name: str) -> ValidationBackend:
		name_normalized = name.lower()
		from .pandas_backend import PandasBackend
		from .polars_backend import PolarsBackend

		if name_normalized == "pandas":
			return PandasBackend()
		if name_normalized == "polars":
			return PolarsBackend()
		raise ValueError(f"Unknown backend '{name}'")


from .pandas_backend import PandasBackend
from .polars_backend import PolarsBackend

__all__ = [
	"BackendFactory",
	"ColumnValidationError",
	"PandasBackend",
	"PolarsBackend",
	"ValidationBackend",
]
