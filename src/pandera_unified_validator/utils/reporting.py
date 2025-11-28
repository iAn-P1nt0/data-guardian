"""Validation reporting with multiple export formats."""

from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, MutableMapping

import pandas as pd
from jinja2 import Template
from rich.console import Console
from rich.panel import Panel
from rich.progress import BarColumn, Progress, TextColumn
from rich.table import Table
from rich.text import Text


@dataclass(frozen=True)
class ValidationReport:
    """Lightweight container describing the outcome of a validation run."""

    is_valid: bool
    errors: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()
    metadata: Mapping[str, Any] = field(default_factory=dict)

    @classmethod
    def ok(
        cls,
        *,
        message: str | None = None,
        stage: str | None = None,
        **metadata: Any,
    ) -> "ValidationReport":
        payload: MutableMapping[str, Any] = dict(metadata)
        if message:
            payload.setdefault("message", message)
        if stage:
            payload.setdefault("stage", stage)
        return cls(True, (), (), payload)

    @classmethod
    def from_exception(
        cls,
        exc: Exception,
        *,
        stage: str | None = None,
    ) -> "ValidationReport":
        message = str(exc).strip() or exc.__class__.__name__
        payload: MutableMapping[str, Any] = {}
        if stage:
            payload["stage"] = stage
        return cls(False, (message,), (), payload)

    def merge(self, other: "ValidationReport") -> "ValidationReport":
        metadata = {**self.metadata, **other.metadata}
        return ValidationReport(
            self.is_valid and other.is_valid,
            self.errors + other.errors,
            self.warnings + other.warnings,
            metadata,
        )

    def with_metadata(self, **metadata: Any) -> "ValidationReport":
        return ValidationReport(self.is_valid, self.errors, self.warnings, {**self.metadata, **metadata})


class ValidationReporter:
    """Generate beautiful, actionable validation reports in multiple formats."""

    def __init__(self, result: Any) -> None:
        """
        Initialize reporter with validation result.

        Args:
            result: ValidationResult from UnifiedValidator
        """
        self.result = result

    def to_console(self, verbose: bool = False, console: Console | None = None) -> None:
        """
        Print formatted validation report to console using rich.

        Args:
            verbose: Include detailed error information
            console: Optional custom Console instance
        """
        console = console or Console()

        # Summary statistics
        total_errors = len(self.result.errors)
        total_warnings = len(self.result.warnings)
        total_suggestions = len(self.result.suggestions)

        # Build summary table
        summary = Table(title="Validation Summary", show_header=True, header_style="bold magenta")
        summary.add_column("Metric", style="cyan", width=20)
        summary.add_column("Value", style="white", width=30)

        status_text = Text("VALID", style="bold green") if self.result.is_valid else Text("INVALID", style="bold red")
        summary.add_row("Status", status_text)
        summary.add_row("Total Errors", str(total_errors))
        summary.add_row("Total Warnings", str(total_warnings))
        summary.add_row("Auto-Fix Suggestions", str(total_suggestions))

        # Add metadata if present
        if self.result.metadata:
            for key, value in self.result.metadata.items():
                if key not in ("stage", "message"):
                    summary.add_row(key.replace("_", " ").title(), str(value))

        console.print(summary)
        console.print()

        # Error breakdown by column
        if total_errors > 0:
            error_by_column = self._group_errors_by_column()
            if error_by_column:
                error_table = Table(title="Errors by Column", show_header=True)
                error_table.add_column("Column", style="yellow")
                error_table.add_column("Count", style="red", justify="right")
                error_table.add_column("Percentage", style="red", justify="right")

                for column, count in error_by_column.most_common(10):
                    pct = (count / total_errors) * 100
                    error_table.add_row(column or "Unknown", str(count), f"{pct:.1f}%")

                console.print(error_table)
                console.print()

            # Top 10 most common errors
            if verbose:
                error_messages = Counter(err.message for err in self.result.errors)
                if error_messages:
                    top_errors = Table(title="Top 10 Error Messages", show_header=True)
                    top_errors.add_column("Error Message", style="red", overflow="fold")
                    top_errors.add_column("Count", style="red", justify="right")

                    for msg, count in error_messages.most_common(10):
                        truncated = msg[:100] + "..." if len(msg) > 100 else msg
                        top_errors.add_row(truncated, str(count))

                    console.print(top_errors)
                    console.print()

        # Warnings
        if total_warnings > 0:
            warnings_panel = Panel(
                "\n".join(f"• {w}" for w in self.result.warnings[:10]),
                title="Warnings",
                border_style="yellow",
            )
            console.print(warnings_panel)
            console.print()

        # Auto-fix suggestions
        if total_suggestions > 0:
            suggestions_table = Table(title="Auto-Fix Suggestions", show_header=True)
            suggestions_table.add_column("Column", style="cyan")
            suggestions_table.add_column("Suggested Fix", style="green", overflow="fold")

            for suggestion in self.result.suggestions[:10]:
                column = suggestion.column or "N/A"
                suggestions_table.add_row(column, suggestion.description)

            console.print(suggestions_table)
            console.print()

    def to_html(self, filepath: Path | str, title: str = "Validation Report") -> None:
        """
        Generate interactive HTML report with charts and tables.

        Args:
            filepath: Output path for HTML file
            title: Report title
        """
        filepath = Path(filepath)

        # Prepare data
        total_errors = len(self.result.errors)
        total_warnings = len(self.result.warnings)
        error_by_column = self._group_errors_by_column()

        # Error distribution data for chart
        error_columns = []
        error_counts = []
        for column, count in error_by_column.most_common(10):
            error_columns.append(column or "Unknown")
            error_counts.append(count)

        # Common error messages
        error_messages = Counter(err.message for err in self.result.errors)
        top_errors = [
            {"message": msg[:150], "count": count}
            for msg, count in error_messages.most_common(10)
        ]

        # Render HTML template
        html_content = self._render_html_template(
            title=title,
            is_valid=self.result.is_valid,
            total_errors=total_errors,
            total_warnings=total_warnings,
            total_suggestions=len(self.result.suggestions),
            error_columns=error_columns,
            error_counts=error_counts,
            top_errors=top_errors,
            warnings=list(self.result.warnings[:20]),
            suggestions=[
                {"column": s.column or "N/A", "description": s.description}
                for s in self.result.suggestions
            ],
            metadata=dict(self.result.metadata),
            timestamp=datetime.now().isoformat(),
        )

        filepath.write_text(html_content, encoding="utf-8")

    def to_json(self, filepath: Path | str, indent: int = 2) -> None:
        """
        Export validation result as JSON.

        Args:
            filepath: Output path for JSON file
            indent: JSON indentation level
        """
        filepath = Path(filepath)

        data = {
            "is_valid": self.result.is_valid,
            "summary": {
                "total_errors": len(self.result.errors),
                "total_warnings": len(self.result.warnings),
                "total_suggestions": len(self.result.suggestions),
            },
            "errors": [
                {
                    "message": err.message,
                    "row": err.row,
                    "column": err.column,
                    "context": dict(err.context) if err.context else {},
                }
                for err in self.result.errors
            ],
            "warnings": list(self.result.warnings),
            "suggestions": [
                {
                    "column": s.column,
                    "description": s.description,
                    "has_fixer": s.fixer is not None,
                }
                for s in self.result.suggestions
            ],
            "metadata": dict(self.result.metadata),
            "timestamp": datetime.now().isoformat(),
        }

        filepath.write_text(json.dumps(data, indent=indent), encoding="utf-8")

    def to_dataframe(self) -> pd.DataFrame:
        """
        Convert errors to DataFrame for analysis.

        Returns:
            DataFrame with columns: row, column, message, context_keys
        """
        if not self.result.errors:
            return pd.DataFrame(columns=["row", "column", "message", "context_keys"])

        records = []
        for err in self.result.errors:
            records.append({
                "row": err.row,
                "column": err.column or "Unknown",
                "message": err.message,
                "context_keys": list(err.context.keys()) if err.context else [],
            })

        return pd.DataFrame(records)

    def _group_errors_by_column(self) -> Counter[str]:
        """Group errors by column name."""
        column_counts: Counter[str] = Counter()
        for err in self.result.errors:
            column_counts[err.column or "Unknown"] += 1
        return column_counts

    def _render_html_template(self, **context: Any) -> str:
        """Render HTML template with validation data."""
        template = Template(HTML_TEMPLATE)
        return template.render(**context)


class MetricsExporter:
    """Export validation metrics to monitoring systems."""

    @staticmethod
    def to_prometheus(metrics: Any) -> str:
        """
        Export metrics in Prometheus format.

        Args:
            metrics: ValidationMetrics from streaming validation

        Returns:
            Prometheus-formatted metrics string
        """
        lines = [
            "# HELP data_guardian_total_rows Total rows validated",
            "# TYPE data_guardian_total_rows counter",
            f"data_guardian_total_rows {metrics.total_rows}",
            "",
            "# HELP data_guardian_valid_rows Valid rows count",
            "# TYPE data_guardian_valid_rows counter",
            f"data_guardian_valid_rows {metrics.valid_rows}",
            "",
            "# HELP data_guardian_invalid_rows Invalid rows count",
            "# TYPE data_guardian_invalid_rows counter",
            f"data_guardian_invalid_rows {metrics.invalid_rows}",
            "",
            "# HELP data_guardian_error_rate Error rate (0.0-1.0)",
            "# TYPE data_guardian_error_rate gauge",
            f"data_guardian_error_rate {metrics.error_rate:.4f}",
            "",
            "# HELP data_guardian_processing_time_seconds Processing time in seconds",
            "# TYPE data_guardian_processing_time_seconds gauge",
            f"data_guardian_processing_time_seconds {metrics.processing_time:.3f}",
            "",
            "# HELP data_guardian_chunks_processed Total chunks processed",
            "# TYPE data_guardian_chunks_processed counter",
            f"data_guardian_chunks_processed {metrics.chunks_processed}",
            "",
        ]

        # Add common errors as labeled metrics
        for error_key, count in list(metrics.common_errors.items())[:10]:
            error_label = error_key.replace('"', '\\"')[:50]
            lines.extend([
                f'data_guardian_common_errors{{error="{error_label}"}} {count}',
            ])

        return "\n".join(lines)

    @staticmethod
    def to_opentelemetry(metrics: Any) -> dict[str, Any]:
        """
        Export metrics for OpenTelemetry.

        Args:
            metrics: ValidationMetrics from streaming validation

        Returns:
            Dictionary compatible with OpenTelemetry format
        """
        return {
            "resource": {
                "service.name": "data-guardian",
                "service.version": "0.1.0",
            },
            "metrics": [
                {
                    "name": "data_guardian.total_rows",
                    "description": "Total rows validated",
                    "unit": "rows",
                    "type": "counter",
                    "value": metrics.total_rows,
                },
                {
                    "name": "data_guardian.valid_rows",
                    "description": "Valid rows count",
                    "unit": "rows",
                    "type": "counter",
                    "value": metrics.valid_rows,
                },
                {
                    "name": "data_guardian.invalid_rows",
                    "description": "Invalid rows count",
                    "unit": "rows",
                    "type": "counter",
                    "value": metrics.invalid_rows,
                },
                {
                    "name": "data_guardian.error_rate",
                    "description": "Error rate (0.0-1.0)",
                    "unit": "1",
                    "type": "gauge",
                    "value": metrics.error_rate,
                },
                {
                    "name": "data_guardian.processing_time",
                    "description": "Processing time in seconds",
                    "unit": "s",
                    "type": "gauge",
                    "value": metrics.processing_time,
                },
                {
                    "name": "data_guardian.chunks_processed",
                    "description": "Total chunks processed",
                    "unit": "chunks",
                    "type": "counter",
                    "value": metrics.chunks_processed,
                },
            ],
            "attributes": {
                "early_terminated": metrics.early_terminated,
                "common_errors_count": len(metrics.common_errors),
            },
            "timestamp": datetime.now().isoformat(),
        }


# HTML Template for validation reports
HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{{ title }}</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            padding: 20px;
            min-height: 100vh;
        }
        .container {
            max-width: 1400px;
            margin: 0 auto;
            background: white;
            border-radius: 12px;
            box-shadow: 0 20px 60px rgba(0,0,0,0.3);
            overflow: hidden;
        }
        .header {
            background: {% if is_valid %}#10b981{% else %}#ef4444{% endif %};
            color: white;
            padding: 40px;
            text-align: center;
        }
        .header h1 {
            font-size: 2.5em;
            margin-bottom: 10px;
        }
        .status-badge {
            display: inline-block;
            padding: 10px 30px;
            background: rgba(255,255,255,0.2);
            border-radius: 25px;
            font-size: 1.2em;
            font-weight: bold;
            margin-top: 10px;
        }
        .summary {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            padding: 40px;
            background: #f9fafb;
        }
        .metric-card {
            background: white;
            padding: 25px;
            border-radius: 10px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
            text-align: center;
        }
        .metric-card h3 {
            color: #6b7280;
            font-size: 0.9em;
            text-transform: uppercase;
            letter-spacing: 1px;
            margin-bottom: 10px;
        }
        .metric-card .value {
            font-size: 2.5em;
            font-weight: bold;
            color: #1f2937;
        }
        .metric-card.errors .value { color: #ef4444; }
        .metric-card.warnings .value { color: #f59e0b; }
        .metric-card.suggestions .value { color: #10b981; }
        .section {
            padding: 40px;
            border-top: 1px solid #e5e7eb;
        }
        .section h2 {
            color: #1f2937;
            margin-bottom: 20px;
            font-size: 1.8em;
        }
        .chart-container {
            position: relative;
            height: 400px;
            margin-bottom: 30px;
        }
        table {
            width: 100%;
            border-collapse: collapse;
            margin-top: 20px;
        }
        th, td {
            padding: 15px;
            text-align: left;
            border-bottom: 1px solid #e5e7eb;
        }
        th {
            background: #f9fafb;
            font-weight: 600;
            color: #374151;
            text-transform: uppercase;
            font-size: 0.85em;
            letter-spacing: 0.5px;
        }
        tr:hover {
            background: #f9fafb;
        }
        .warning-list {
            list-style: none;
            padding: 0;
        }
        .warning-list li {
            padding: 15px;
            background: #fef3c7;
            border-left: 4px solid #f59e0b;
            margin-bottom: 10px;
            border-radius: 4px;
        }
        .suggestion-item {
            padding: 15px;
            background: #d1fae5;
            border-left: 4px solid #10b981;
            margin-bottom: 10px;
            border-radius: 4px;
        }
        .suggestion-item strong {
            color: #065f46;
        }
        .footer {
            padding: 20px 40px;
            background: #f9fafb;
            text-align: center;
            color: #6b7280;
            font-size: 0.9em;
        }
        .export-buttons {
            margin-top: 20px;
        }
        .btn {
            display: inline-block;
            padding: 12px 24px;
            margin: 5px;
            background: #667eea;
            color: white;
            text-decoration: none;
            border-radius: 6px;
            font-weight: 600;
            transition: background 0.3s;
        }
        .btn:hover {
            background: #5a67d8;
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>{{ title }}</h1>
            <div class="status-badge">
                {% if is_valid %}✓ VALID{% else %}✗ INVALID{% endif %}
            </div>
        </div>

        <div class="summary">
            <div class="metric-card errors">
                <h3>Total Errors</h3>
                <div class="value">{{ total_errors }}</div>
            </div>
            <div class="metric-card warnings">
                <h3>Total Warnings</h3>
                <div class="value">{{ total_warnings }}</div>
            </div>
            <div class="metric-card suggestions">
                <h3>Auto-Fix Suggestions</h3>
                <div class="value">{{ total_suggestions }}</div>
            </div>
        </div>

        {% if error_columns %}
        <div class="section">
            <h2>Error Distribution by Column</h2>
            <div class="chart-container">
                <canvas id="errorChart"></canvas>
            </div>
        </div>
        {% endif %}

        {% if top_errors %}
        <div class="section">
            <h2>Most Common Errors</h2>
            <table>
                <thead>
                    <tr>
                        <th>Error Message</th>
                        <th style="text-align: right; width: 100px;">Count</th>
                    </tr>
                </thead>
                <tbody>
                    {% for error in top_errors %}
                    <tr>
                        <td>{{ error.message }}</td>
                        <td style="text-align: right;">{{ error.count }}</td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
        </div>
        {% endif %}

        {% if warnings %}
        <div class="section">
            <h2>Warnings</h2>
            <ul class="warning-list">
                {% for warning in warnings %}
                <li>{{ warning }}</li>
                {% endfor %}
            </ul>
        </div>
        {% endif %}

        {% if suggestions %}
        <div class="section">
            <h2>Auto-Fix Suggestions</h2>
            {% for suggestion in suggestions %}
            <div class="suggestion-item">
                <strong>{{ suggestion.column }}</strong>: {{ suggestion.description }}
            </div>
            {% endfor %}
        </div>
        {% endif %}

        <div class="footer">
            <p>Generated by Data Guardian v0.1.0 on {{ timestamp }}</p>
            <div class="export-buttons">
                <a href="#" class="btn" onclick="alert('Export to CSV functionality would be implemented here')">Export to CSV</a>
                <a href="#" class="btn" onclick="alert('Export to Excel functionality would be implemented here')">Export to Excel</a>
            </div>
        </div>
    </div>

    <script>
        {% if error_columns %}
        const ctx = document.getElementById('errorChart');
        new Chart(ctx, {
            type: 'bar',
            data: {
                labels: {{ error_columns | tojson }},
                datasets: [{
                    label: 'Error Count',
                    data: {{ error_counts | tojson }},
                    backgroundColor: 'rgba(239, 68, 68, 0.8)',
                    borderColor: 'rgba(239, 68, 68, 1)',
                    borderWidth: 1
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        display: false
                    },
                    title: {
                        display: true,
                        text: 'Errors by Column',
                        font: {
                            size: 16
                        }
                    }
                },
                scales: {
                    y: {
                        beginAtZero: true,
                        ticks: {
                            precision: 0
                        }
                    }
                }
            }
        });
        {% endif %}
    </script>
</body>
</html>
"""
