"""Severity levels and the one place their ordering is defined.

Four levels, in increasing order: INFO, WARNING, ERROR, CRITICAL.

Behaviour attached to each level (section 10 of the specification):

    INFO      recorded only. Never opens an incident, never alerts.
    WARNING   recorded, opens an incident, alerts. The pipeline continues -
              a contract may legitimately tolerate a warning.
    ERROR     recorded, opens an incident, alerts, and marks the dataset
              unhealthy. Downstream tasks for that dataset are skipped.
    CRITICAL  all of the above, and the quality gate FAILS, which fails the
              Airflow task and stops the downstream pipeline.

The comparison operators exist so no caller has to re-invent the ordering with
string comparison - "ERROR" < "WARNING" lexicographically, which is exactly
the kind of bug this module exists to prevent.
"""

from __future__ import annotations

from collections.abc import Iterable
from enum import Enum


class Severity(str, Enum):
    """A severity level.

    Subclasses ``str`` so a value can be written straight into a psycopg
    parameter or a JSON payload without conversion, while still comparing by
    rank rather than alphabetically.
    """

    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"

    @property
    def rank(self) -> int:
        return _RANKS[self]

    # -- ordering ---------------------------------------------------------
    # `str` already defines these, so they must be overridden explicitly:
    # inheriting str's lexicographic comparison is the bug, not the feature.
    def __lt__(self, other: object) -> bool:
        if isinstance(other, Severity):
            return self.rank < other.rank
        return NotImplemented

    def __le__(self, other: object) -> bool:
        if isinstance(other, Severity):
            return self.rank <= other.rank
        return NotImplemented

    def __gt__(self, other: object) -> bool:
        if isinstance(other, Severity):
            return self.rank > other.rank
        return NotImplemented

    def __ge__(self, other: object) -> bool:
        if isinstance(other, Severity):
            return self.rank >= other.rank
        return NotImplemented

    # -- behaviour --------------------------------------------------------
    @property
    def opens_incident(self) -> bool:
        """INFO is informational only and must not create incident noise."""
        return self.rank >= Severity.WARNING.rank

    @property
    def alerts(self) -> bool:
        return self.rank >= Severity.WARNING.rank

    @property
    def marks_unhealthy(self) -> bool:
        return self.rank >= Severity.ERROR.rank

    @property
    def fails_gate(self) -> bool:
        """Only CRITICAL stops the pipeline."""
        return self is Severity.CRITICAL

    # -- parsing ----------------------------------------------------------
    @classmethod
    def parse(cls, value: str | Severity, default: Severity | None = None) -> Severity:
        """Parse a severity from a contract file.

        Contracts are hand-written YAML, so the value arrives as free text.
        An unknown level is an error rather than a silent downgrade: treating
        ``severity: criticial`` as INFO would quietly disable a gate.
        """
        if isinstance(value, Severity):
            return value
        if value is None:
            if default is not None:
                return default
            raise ValueError("severity is required and no default was given")
        key = str(value).strip().upper()
        try:
            return cls(key)
        except ValueError as exc:
            valid = ", ".join(level.value for level in cls)
            raise ValueError(
                f"unknown severity {value!r}; expected one of {valid}"
            ) from exc


_RANKS: dict[Severity, int] = {
    Severity.INFO: 0,
    Severity.WARNING: 1,
    Severity.ERROR: 2,
    Severity.CRITICAL: 3,
}


def max_severity(
    severities: Iterable[Severity | str], default: Severity = Severity.INFO
) -> Severity:
    """Highest severity in an iterable, or ``default`` when it is empty.

    An empty iterable means "nothing was found", which is INFO - not a pass
    and not a failure. Callers that need to distinguish "no findings" from
    "no measurement at all" must check that themselves; see
    ``src/observability/quality_score.py``.
    """
    highest = default
    for item in severities:
        level = Severity.parse(item)
        if level.rank > highest.rank:
            highest = level
    return highest
