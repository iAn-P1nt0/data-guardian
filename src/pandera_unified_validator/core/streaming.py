"""Streaming validation for large datasets that don't fit in memory."""

from __future__ import annotations

import asyncio
import json
import time
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import (
    Any,
    AsyncIterator,
    Callable,
    Dict,
    Iterator,
    List,
    Mapping,
    Optional,
    Protocol,
    TypeVar,
    Union,
)

import pandas as pd

from .schema import UnifiedSchema, ValidationSchema
from ..utils.reporting import ValidationReport

T = TypeVar("T")


@dataclass
class ValidationMetrics:
    """Aggregated metrics for streaming validation."""

    total_rows: int = 0
    valid_rows: int = 0
    invalid_rows: int = 0
    error_rate: float = 0.0
    common_errors: Dict[str, int] = field(default_factory=dict)
    processing_time: float = 0.0
    chunks_processed: int = 0
    early_terminated: bool = False

    def update(self, chunk_valid: int, chunk_invalid: int, errors: List[str]) -> None:
        """Update metrics with chunk results."""
        self.total_rows += chunk_valid + chunk_invalid
        self.valid_rows += chunk_valid
        self.invalid_rows += chunk_invalid
        self.chunks_processed += 1

        for error in errors:
            error_key = self._normalize_error(error)
            self.common_errors[error_key] = self.common_errors.get(error_key, 0) + 1

        if self.total_rows > 0:
            self.error_rate = self.invalid_rows / self.total_rows

    def _normalize_error(self, error: str) -> str:
        """Normalize error message for grouping."""
        lines = error.strip().split("\n")
        if lines:
            first_line = lines[0]
            if len(first_line) > 100:
                return first_line[:100] + "..."
            return first_line
        return error[:100] if len(error) > 100 else error

    def to_dict(self) -> Dict[str, Any]:
        """Serialize metrics to dictionary."""
        return {
            "total_rows": self.total_rows,
            "valid_rows": self.valid_rows,
            "invalid_rows": self.invalid_rows,
            "error_rate": round(self.error_rate, 4),
            "common_errors": dict(
                sorted(self.common_errors.items(), key=lambda x: -x[1])[:10]
            ),
            "processing_time": round(self.processing_time, 3),
            "chunks_processed": self.chunks_processed,
            "early_terminated": self.early_terminated,
        }


@dataclass
class StreamingResult:
    """Final result of streaming validation."""

    is_valid: bool
    metrics: ValidationMetrics
    schema_name: str
    source: str
    errors_sample: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Serialize result to dictionary."""
        return {
            "is_valid": self.is_valid,
            "schema_name": self.schema_name,
            "source": self.source,
            "metrics": self.metrics.to_dict(),
            "errors_sample": self.errors_sample[:10],
        }


class ProgressCallback(Protocol):
    """Protocol for progress reporting callbacks."""

    def __call__(self, metrics: ValidationMetrics) -> None:
        """Report current validation progress."""
        ...


class StreamingValidator:
    """Validates large datasets in chunks with async support."""

    def __init__(
        self,
        schema: UnifiedSchema | ValidationSchema,
        *,
        chunk_size: int = 10000,
        error_threshold: float = 0.05,
        max_errors_sample: int = 100,
    ) -> None:
        """
        Initialize streaming validator.

        Args:
            schema: Schema to validate against
            chunk_size: Number of rows per chunk
            error_threshold: Stop if error rate exceeds this (0.0-1.0)
            max_errors_sample: Maximum errors to collect for reporting
        """
        if isinstance(schema, UnifiedSchema):
            self._schema = schema.to_validation_schema()
            self._schema_name = schema.name
        else:
            self._schema = schema
            self._schema_name = schema.name

        self.chunk_size = chunk_size
        self.error_threshold = error_threshold
        self.max_errors_sample = max_errors_sample

    async def validate_csv(
        self,
        filepath: Path | str,
        *,
        report_callback: Optional[ProgressCallback] = None,
        encoding: str = "utf-8",
        **read_csv_kwargs: Any,
    ) -> StreamingResult:
        """
        Validate CSV file in chunks.

        Args:
            filepath: Path to CSV file
            report_callback: Optional callback for progress reporting
            encoding: File encoding
            **read_csv_kwargs: Additional arguments for pd.read_csv
        """
        filepath = Path(filepath)
        source = str(filepath)

        async def chunk_iterator() -> AsyncIterator[pd.DataFrame]:
            reader = pd.read_csv(
                filepath,
                chunksize=self.chunk_size,
                encoding=encoding,
                **read_csv_kwargs,
            )
            for chunk in reader:
                yield chunk
                await asyncio.sleep(0)  # Yield control

        return await self._validate_chunks(chunk_iterator(), source, report_callback)

    async def validate_jsonl(
        self,
        filepath: Path | str,
        *,
        report_callback: Optional[ProgressCallback] = None,
        encoding: str = "utf-8",
    ) -> StreamingResult:
        """
        Validate JSON Lines file in chunks.

        Args:
            filepath: Path to JSONL file
            report_callback: Optional callback for progress reporting
            encoding: File encoding
        """
        filepath = Path(filepath)
        source = str(filepath)

        async def chunk_iterator() -> AsyncIterator[pd.DataFrame]:
            buffer: List[Dict[str, Any]] = []
            with open(filepath, "r", encoding=encoding) as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        record = json.loads(line)
                        buffer.append(record)
                    except json.JSONDecodeError:
                        buffer.append({"__parse_error__": line})

                    if len(buffer) >= self.chunk_size:
                        yield pd.DataFrame(buffer)
                        buffer = []
                        await asyncio.sleep(0)

            if buffer:
                yield pd.DataFrame(buffer)

        return await self._validate_chunks(chunk_iterator(), source, report_callback)

    async def validate_parquet(
        self,
        filepath: Path | str,
        *,
        report_callback: Optional[ProgressCallback] = None,
        batch_size: int | None = None,
    ) -> StreamingResult:
        """
        Validate Parquet file in chunks using row groups.

        Args:
            filepath: Path to Parquet file
            report_callback: Optional callback for progress reporting
            batch_size: Override chunk size for parquet batches
        """
        try:
            import pyarrow.parquet as pq
        except ImportError as exc:
            raise ImportError(
                "pyarrow is required for Parquet support. "
                "Install with: pip install pyarrow"
            ) from exc

        filepath = Path(filepath)
        source = str(filepath)
        batch = batch_size or self.chunk_size

        async def chunk_iterator() -> AsyncIterator[pd.DataFrame]:
            parquet_file = pq.ParquetFile(filepath)
            for batch_df in parquet_file.iter_batches(batch_size=batch):
                yield batch_df.to_pandas()
                await asyncio.sleep(0)

        return await self._validate_chunks(chunk_iterator(), source, report_callback)

    async def validate_stream(
        self,
        data_stream: AsyncIterator[Mapping[str, Any]],
        *,
        source: str = "stream",
        report_callback: Optional[ProgressCallback] = None,
    ) -> StreamingResult:
        """
        Validate async data stream.

        Args:
            data_stream: Async iterator yielding dictionaries
            source: Source identifier for reporting
            report_callback: Optional callback for progress reporting
        """

        async def chunk_iterator() -> AsyncIterator[pd.DataFrame]:
            buffer: List[Dict[str, Any]] = []
            async for record in data_stream:
                buffer.append(dict(record))
                if len(buffer) >= self.chunk_size:
                    yield pd.DataFrame(buffer)
                    buffer = []

            if buffer:
                yield pd.DataFrame(buffer)

        return await self._validate_chunks(chunk_iterator(), source, report_callback)

    async def validate_db_cursor(
        self,
        query_result: Any,
        *,
        source: str = "database",
        report_callback: Optional[ProgressCallback] = None,
        column_names: List[str] | None = None,
    ) -> StreamingResult:
        """
        Validate database cursor results.

        Args:
            query_result: SQLAlchemy Result or cursor-like object
            source: Source identifier for reporting
            report_callback: Optional callback for progress reporting
            column_names: Column names if not available from cursor
        """

        async def chunk_iterator() -> AsyncIterator[pd.DataFrame]:
            buffer: List[Dict[str, Any]] = []
            columns = column_names

            if hasattr(query_result, "keys"):
                columns = list(query_result.keys())

            for row in query_result:
                if hasattr(row, "_asdict"):
                    buffer.append(row._asdict())
                elif hasattr(row, "_mapping"):
                    buffer.append(dict(row._mapping))
                elif columns:
                    buffer.append(dict(zip(columns, row)))
                else:
                    buffer.append({"__row__": row})

                if len(buffer) >= self.chunk_size:
                    yield pd.DataFrame(buffer)
                    buffer = []
                    await asyncio.sleep(0)

            if buffer:
                yield pd.DataFrame(buffer)

        return await self._validate_chunks(chunk_iterator(), source, report_callback)

    def validate_csv_sync(
        self,
        filepath: Path | str,
        *,
        report_callback: Optional[ProgressCallback] = None,
        encoding: str = "utf-8",
        **read_csv_kwargs: Any,
    ) -> StreamingResult:
        """Synchronous version of validate_csv."""
        return asyncio.run(
            self.validate_csv(
                filepath,
                report_callback=report_callback,
                encoding=encoding,
                **read_csv_kwargs,
            )
        )

    def validate_jsonl_sync(
        self,
        filepath: Path | str,
        *,
        report_callback: Optional[ProgressCallback] = None,
        encoding: str = "utf-8",
    ) -> StreamingResult:
        """Synchronous version of validate_jsonl."""
        return asyncio.run(
            self.validate_jsonl(
                filepath,
                report_callback=report_callback,
                encoding=encoding,
            )
        )

    def validate_parquet_sync(
        self,
        filepath: Path | str,
        *,
        report_callback: Optional[ProgressCallback] = None,
        batch_size: int | None = None,
    ) -> StreamingResult:
        """Synchronous version of validate_parquet."""
        return asyncio.run(
            self.validate_parquet(
                filepath,
                report_callback=report_callback,
                batch_size=batch_size,
            )
        )

    async def _validate_chunks(
        self,
        chunks: AsyncIterator[pd.DataFrame],
        source: str,
        report_callback: Optional[ProgressCallback],
    ) -> StreamingResult:
        """Core validation loop for chunk processing."""
        metrics = ValidationMetrics()
        errors_sample: List[str] = []
        start_time = time.perf_counter()
        overall_valid = True

        async for chunk in chunks:
            chunk_errors: List[str] = []
            chunk_valid = 0
            chunk_invalid = 0

            report = self._schema.validate_dataframe(chunk)

            if report.is_valid:
                chunk_valid = len(chunk)
            else:
                overall_valid = False
                chunk_errors.extend(report.errors)

                if self._schema.record_model is not None:
                    for idx, row in chunk.iterrows():
                        row_report = self._schema.validate_records([row.to_dict()])
                        if row_report.is_valid:
                            chunk_valid += 1
                        else:
                            chunk_invalid += 1
                            if len(errors_sample) < self.max_errors_sample:
                                errors_sample.extend(row_report.errors)
                else:
                    chunk_invalid = len(chunk)
                    if len(errors_sample) < self.max_errors_sample:
                        errors_sample.extend(report.errors)

            metrics.update(chunk_valid, chunk_invalid, chunk_errors)
            metrics.processing_time = time.perf_counter() - start_time

            if report_callback is not None:
                report_callback(metrics)

            if metrics.error_rate > self.error_threshold:
                metrics.early_terminated = True
                break

        metrics.processing_time = time.perf_counter() - start_time

        return StreamingResult(
            is_valid=overall_valid and metrics.invalid_rows == 0,
            metrics=metrics,
            schema_name=self._schema_name,
            source=source,
            errors_sample=errors_sample[: self.max_errors_sample],
        )


async def validate_csv_streaming(
    filepath: Path | str,
    schema: UnifiedSchema | ValidationSchema,
    *,
    chunk_size: int = 10000,
    error_threshold: float = 0.05,
    report_callback: Optional[ProgressCallback] = None,
    **read_csv_kwargs: Any,
) -> StreamingResult:
    """
    Convenience function for streaming CSV validation.

    Args:
        filepath: Path to CSV file
        schema: Schema to validate against
        chunk_size: Number of rows per chunk
        error_threshold: Stop if error rate exceeds this
        report_callback: Optional callback for progress reporting
        **read_csv_kwargs: Additional arguments for pd.read_csv

    Returns:
        StreamingResult with validation outcome and metrics
    """
    validator = StreamingValidator(
        schema,
        chunk_size=chunk_size,
        error_threshold=error_threshold,
    )
    return await validator.validate_csv(
        filepath,
        report_callback=report_callback,
        **read_csv_kwargs,
    )


def validate_csv_streaming_sync(
    filepath: Path | str,
    schema: UnifiedSchema | ValidationSchema,
    *,
    chunk_size: int = 10000,
    error_threshold: float = 0.05,
    report_callback: Optional[ProgressCallback] = None,
    **read_csv_kwargs: Any,
) -> StreamingResult:
    """Synchronous version of validate_csv_streaming."""
    return asyncio.run(
        validate_csv_streaming(
            filepath,
            schema,
            chunk_size=chunk_size,
            error_threshold=error_threshold,
            report_callback=report_callback,
            **read_csv_kwargs,
        )
    )
