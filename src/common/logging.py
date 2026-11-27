"""Logging setup shared by the CLI entrypoints, the services and the DAG.

Two requirements shape this module:

* Airflow captures a task's stdout/stderr into the task log, so the platform
  must not install handlers that bypass them or duplicate records.
* Nothing must ever log a credential. ``redact`` is applied to the few places
  that handle connection strings.
"""

from __future__ import annotations

import logging
import os
import re
import sys
from typing import Any

_CONFIGURED = False

# Matches `password=...` in a libpq conninfo string and the password segment
# of a URL. Both forms appear in driver error messages, which is exactly where
# a secret would otherwise leak into a log.
_PASSWORD_PATTERNS = (
    re.compile(r"(password\s*=\s*)(\S+)", re.IGNORECASE),
    re.compile(r"(://[^:/@\s]+:)([^@/\s]+)(@)"),
)


def redact(text: str) -> str:
    """Remove password material from a string before it is logged."""
    if not text:
        return text
    cleaned = _PASSWORD_PATTERNS[0].sub(r"\1***", text)
    cleaned = _PASSWORD_PATTERNS[1].sub(r"\1***\3", cleaned)
    return cleaned


class _RedactingFormatter(logging.Formatter):
    """Formatter that redacts the fully rendered message.

    Redacting at format time rather than at call time means a password that
    reaches the log through an exception's own text - not through one of our
    f-strings - is still removed.
    """

    def format(self, record: logging.LogRecord) -> str:
        return redact(super().format(record))


def configure_logging(level: str | None = None) -> None:
    """Install a single stream handler on the root logger.

    Idempotent: calling it from several entrypoints in one process (the API
    imports modules that also configure logging) must not double every line.
    """
    global _CONFIGURED
    if _CONFIGURED:
        return

    resolved = (level or os.environ.get("DQ_LOG_LEVEL") or "INFO").upper()
    numeric = getattr(logging, resolved, logging.INFO)

    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setFormatter(
        _RedactingFormatter(
            fmt="%(asctime)s %(levelname)-8s %(name)s | %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S%z",
        )
    )

    root = logging.getLogger()
    root.setLevel(numeric)
    # Replace rather than append: in the Airflow task process a handler is
    # already present, and appending ours would duplicate every record.
    for existing in list(root.handlers):
        if getattr(existing, "_dq_platform", False):
            root.removeHandler(existing)
    handler._dq_platform = True  # type: ignore[attr-defined]
    root.addHandler(handler)

    # Third-party loggers that are noisy at INFO and say nothing useful here.
    for noisy in ("botocore", "boto3", "urllib3", "s3transfer", "great_expectations"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    _CONFIGURED = True


def get_logger(name: str) -> logging.Logger:
    """Return a configured logger."""
    configure_logging()
    return logging.getLogger(name)


def log_context(**fields: Any) -> str:
    """Render structured key/value context into a single log-friendly string.

    Kept deliberately simple - a full structured-logging stack would be more
    than this platform needs, and Airflow's log view is line oriented.
    """
    return " ".join(f"{key}={value}" for key, value in fields.items() if value is not None)
