"""Command-line interface for pandera-unified-validator."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Optional

import pandas as pd
import typer
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn

from . import (
    SchemaBuilder,
    StreamingValidator,
    UnifiedValidator,
    ValidationReporter,
    __version__,
)
from .profiling import DataProfiler

app = typer.Typer(
    name="puv",
    help="pandera-unified-validator: Advanced data validation and profiling CLI",
    add_completion=False,
)
console = Console()


def version_callback(value: bool) -> None:
    """Print version and exit."""
    if value:
        console.print(f"pandera-unified-validator version {__version__}")
        raise typer.Exit()


@app.callback()
def main(
    version: Optional[bool] = typer.Option(
        None,
        "--version",
        "-v",
        help="Show version and exit",
        callback=version_callback,
        is_eager=True,
    ),
) -> None:
    """pandera-unified-validator: Advanced data validation and profiling."""
    pass


@app.command()
def validate(
    file: Path = typer.Argument(..., help="CSV file to validate"),
    schema: Path = typer.Argument(..., help="Schema JSON file"),
    output: Path = typer.Option(
        Path("validation_report.html"),
        "--output",
        "-o",
        help="Output report file (HTML or JSON)",
    ),
    auto_fix: bool = typer.Option(
        False,
        "--auto-fix",
        help="Enable auto-fix suggestions",
    ),
    streaming: bool = typer.Option(
        False,
        "--streaming",
        help="Use streaming validation for large files",
    ),
    chunk_size: int = typer.Option(
        10000,
        "--chunk-size",
        help="Chunk size for streaming validation",
    ),
    error_threshold: float = typer.Option(
        0.05,
        "--error-threshold",
        help="Error rate threshold for streaming (0.0-1.0)",
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose",
        help="Verbose output",
    ),
) -> None:
    """
    Validate a CSV file against a schema.

    Example:
        puv validate data.csv schema.json --auto-fix --output report.html
    """
    try:
        # Load schema
        console.print(f"[cyan]Loading schema from {schema}...[/cyan]")
        with open(schema, "r") as f:
            schema_data = json.load(f)

        # Build schema from JSON
        builder = SchemaBuilder(schema_data.get("name", "schema"))
        for col_name, col_spec in schema_data.get("columns", {}).items():
            builder.add_column(
                col_name,
                eval(col_spec["dtype"]),  # Convert string to type
                nullable=col_spec.get("nullable", True),
                unique=col_spec.get("unique", False),
                ge=col_spec.get("ge"),
                le=col_spec.get("le"),
                pattern=col_spec.get("pattern"),
            )
        built_schema = builder.build()
        console.print(f"[green]✓[/green] Schema loaded with {len(built_schema.columns)} columns")

        # Validate
        if streaming:
            # Streaming validation
            console.print(f"[cyan]Validating {file} (streaming mode)...[/cyan]")

            def progress_callback(metrics):
                console.print(
                    f"Processed: {metrics.total_rows:,} rows | "
                    f"Valid: {metrics.valid_rows:,} | "
                    f"Invalid: {metrics.invalid_rows:,} | "
                    f"Error rate: {metrics.error_rate:.2%}"
                )

            validator = StreamingValidator(
                built_schema,
                chunk_size=chunk_size,
                error_threshold=error_threshold,
            )
            result = validator.validate_csv_sync(
                file,
                report_callback=progress_callback if verbose else None,
            )

            console.print(
                f"\n[{'green' if result.is_valid else 'red'}]"
                f"Validation {'PASSED' if result.is_valid else 'FAILED'}[/]"
            )
            console.print(f"Total rows: {result.metrics.total_rows:,}")
            console.print(f"Invalid rows: {result.metrics.invalid_rows:,}")
            console.print(f"Processing time: {result.metrics.processing_time:.2f}s")

            # Export metrics
            if output.suffix == ".json":
                output.write_text(json.dumps(result.to_dict(), indent=2))
                console.print(f"[green]✓[/green] Report saved to {output}")

        else:
            # In-memory validation
            console.print(f"[cyan]Loading data from {file}...[/cyan]")
            data = pd.read_csv(file)
            console.print(f"[green]✓[/green] Loaded {len(data):,} rows")

            console.print("[cyan]Validating data...[/cyan]")
            validator = UnifiedValidator(
                built_schema.to_validation_schema(),
                lazy=True,
                auto_fix=auto_fix,
            )
            result = validator.validate(data)

            console.print(
                f"\n[{'green' if result.is_valid else 'red'}]"
                f"Validation {'PASSED' if result.is_valid else 'FAILED'}[/]"
            )
            console.print(f"Errors: {len(result.errors)}")
            console.print(f"Warnings: {len(result.warnings)}")
            console.print(f"Suggestions: {len(result.suggestions)}")

            # Generate report
            reporter = ValidationReporter(result)

            if verbose:
                console.print("\n[bold]Validation Summary:[/bold]")
                reporter.to_console(verbose=True, console=console)

            # Export report
            if output.suffix == ".html":
                reporter.to_html(output)
                console.print(f"[green]✓[/green] HTML report saved to {output}")
            elif output.suffix == ".json":
                reporter.to_json(output)
                console.print(f"[green]✓[/green] JSON report saved to {output}")

    except FileNotFoundError as e:
        console.print(f"[red]Error: File not found: {e.filename}[/red]")
        raise typer.Exit(1)
    except Exception as e:
        console.print(f"[red]Error: {str(e)}[/red]")
        if verbose:
            console.print_exception()
        raise typer.Exit(1)


@app.command()
def profile(
    file: Path = typer.Argument(..., help="CSV file to profile"),
    output: Path = typer.Option(
        Path("profile_report.html"),
        "--output",
        "-o",
        help="Output HTML report file",
    ),
    minimal: bool = typer.Option(
        True,
        "--minimal/--full",
        help="Generate minimal or full profile report",
    ),
    infer_schema: bool = typer.Option(
        False,
        "--infer-schema",
        help="Infer validation schema from profile",
    ),
    schema_output: Optional[Path] = typer.Option(
        None,
        "--schema-output",
        help="Save inferred schema to JSON file",
    ),
) -> None:
    """
    Generate a data profile report.

    Example:
        puv profile data.csv --output profile.html --infer-schema --schema-output schema.json
    """
    try:
        console.print(f"[cyan]Loading data from {file}...[/cyan]")
        data = pd.read_csv(file)
        console.print(f"[green]✓[/green] Loaded {len(data):,} rows, {len(data.columns)} columns")

        console.print("[cyan]Generating profile report...[/cyan]")
        profiler = DataProfiler()

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            task = progress.add_task("Profiling data...", total=None)
            report = profiler.profile(data, minimal=minimal)
            progress.update(task, completed=True)

        console.print(f"[green]✓[/green] Profile generated")
        console.print(f"Quality score: {report.quality_score.overall:.2%}")
        console.print(f"Completeness: {report.quality_score.completeness:.2%}")
        console.print(f"Uniqueness: {report.quality_score.uniqueness:.2%}")

        # Save HTML report
        profiler.to_html(report, output)
        console.print(f"[green]✓[/green] HTML report saved to {output}")

        # Infer schema if requested
        if infer_schema:
            from .profiling import infer_constraints_from_profile

            console.print("[cyan]Inferring validation schema...[/cyan]")
            schema = infer_constraints_from_profile(report)
            console.print(f"[green]✓[/green] Schema inferred with {len(schema.columns)} columns")

            if schema_output:
                schema_output.write_text(schema.to_json())
                console.print(f"[green]✓[/green] Schema saved to {schema_output}")
            else:
                console.print("\n[bold]Inferred Schema:[/bold]")
                console.print(schema.to_json(indent=2))

    except FileNotFoundError as e:
        console.print(f"[red]Error: File not found: {e.filename}[/red]")
        raise typer.Exit(1)
    except Exception as e:
        console.print(f"[red]Error: {str(e)}[/red]")
        console.print_exception()
        raise typer.Exit(1)


@app.command()
def schema(
    data: Path = typer.Argument(..., help="CSV file to analyze"),
    output: Path = typer.Option(
        Path("schema.json"),
        "--output",
        "-o",
        help="Output schema JSON file",
    ),
    infer_constraints: bool = typer.Option(
        True,
        "--infer-constraints/--no-constraints",
        help="Infer constraints from data",
    ),
) -> None:
    """
    Generate a validation schema from a CSV file.

    Example:
        puv schema data.csv --output schema.json
    """
    try:
        console.print(f"[cyan]Loading data from {data}...[/cyan]")
        df = pd.read_csv(data)
        console.print(f"[green]✓[/green] Loaded {len(df):,} rows, {len(df.columns)} columns")

        console.print("[cyan]Inferring schema...[/cyan]")
        from .core.schema import SchemaConverter

        schema = SchemaConverter.infer_from_dataframe(
            df,
            name=data.stem,
            infer_constraints=infer_constraints,
        )

        console.print(f"[green]✓[/green] Schema inferred with {len(schema.columns)} columns")

        # Save schema
        output.write_text(schema.to_json(indent=2))
        console.print(f"[green]✓[/green] Schema saved to {output}")

        # Display schema summary
        console.print("\n[bold]Schema Summary:[/bold]")
        for col_name, col_spec in schema.columns.items():
            constraints = []
            if not col_spec.nullable:
                constraints.append("required")
            if col_spec.unique:
                constraints.append("unique")
            if col_spec.ge is not None:
                constraints.append(f"≥{col_spec.ge}")
            if col_spec.le is not None:
                constraints.append(f"≤{col_spec.le}")

            console.print(
                f"  • {col_name}: {col_spec.dtype.__name__ if isinstance(col_spec.dtype, type) else col_spec.dtype}"
                + (f" ({', '.join(constraints)})" if constraints else "")
            )

    except FileNotFoundError as e:
        console.print(f"[red]Error: File not found: {e.filename}[/red]")
        raise typer.Exit(1)
    except Exception as e:
        console.print(f"[red]Error: {str(e)}[/red]")
        console.print_exception()
        raise typer.Exit(1)


if __name__ == "__main__":
    app()
