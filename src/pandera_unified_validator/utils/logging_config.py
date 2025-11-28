"""Logging configuration for pandera-unified-validator."""

from __future__ import annotations

import logging
import sys
from typing import Any

import structlog


def configure_logging(
    level: str | int = logging.INFO,
    json_logs: bool = False,
    include_timestamp: bool = True,
) -> None:
    """
    Configure structured logging for pandera-unified-validator.

    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        json_logs: If True, output logs as JSON. Otherwise, use console format.
        include_timestamp: Include timestamps in log output

    Example:
        >>> from pandera_unified_validator.utils.logging_config import configure_logging
        >>> configure_logging(level="DEBUG", json_logs=False)
    """
    # Convert string level to logging constant
    if isinstance(level, str):
        level = getattr(logging, level.upper(), logging.INFO)

    # Configure standard logging
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=level,
    )

    # Determine processors based on output format
    processors: list[Any] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]

    if include_timestamp:
        processors.append(structlog.processors.TimeStamper(fmt="iso"))

    if json_logs:
        # JSON output for production
        processors.append(structlog.processors.JSONRenderer())
    else:
        # Console output for development
        processors.extend([
            structlog.dev.set_exc_info,
            structlog.dev.ConsoleRenderer(colors=True),
        ])

    # Configure structlog
    structlog.configure(
        processors=processors,
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str | None = None) -> structlog.stdlib.BoundLogger:
    """
    Get a configured logger instance.

    Args:
        name: Logger name (typically __name__)

    Returns:
        Configured structlog logger

    Example:
        >>> logger = get_logger(__name__)
        >>> logger.info("validation_started", schema="user_schema", rows=1000)
    """
    return structlog.get_logger(name)


# Default configuration - called on module import
configure_logging()
