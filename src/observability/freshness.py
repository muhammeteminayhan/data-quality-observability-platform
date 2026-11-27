"""Freshness: how old is the newest data in a dataset.

Design decisions that matter
----------------------------
1. **The dataset own clock comes first.** The contract names an event
   timestamp and an ingestion timestamp. The event timestamp is preferred,
   because an upstream that keeps loading on schedule while delivering
   week-old events is stale in a way an ingestion time cannot show. The source
   actually used is recorded on every row, so a freshness number is never
   ambiguous about what it measured.

2. **A missing timestamp is `unknown`, never `ok`.** If a dataset exposes no
   usable timestamp, or the column is entirely NULL, or the partition is
   absent, the check reports status `unknown` with the severity the contract
   chose in `on_missing_timestamp`. Reporting such a dataset as fresh would
   turn a broken pipeline into a green dashboard.

3. **Reference time is explicit.** Lag is measured against a reference instant
   passed in by the caller (the DAG logical date, or now()), not against an
   implicit clock inside this module, so a backfill measures freshness as of
   the partition it is rebuilding rather than as of today.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime

from psycopg import sql

from src.common.db import fetch_one, qualified_identifier, source_connection
from src.common.logging import get_logger
from src.common.severity import Severity
from src.common.storage import object_last_modified, partition_prefix, read_partition
from src.contracts.models import Contract

LOGGER = get_logger(__name__)


@dataclass
class FreshnessResult:
    dataset: str
    timestamp_column: str | None
    timestamp_source: str  # event_time | ingestion_time | object_mtime | unavailable
    max_timestamp: datetime | None
    reference_time: datetime
    lag_seconds: int | None
    warning_after_seconds: int | None
    critical_after_seconds: int | None
    status: str  # ok | warning | critical | unknown
    severity: Severity
    detail: str


def _classify(
    lag_seconds: int | None,
    warn: int | None,
    crit: int | None,
) -> tuple[str, Severity]:
    if lag_seconds is None:
        return "unknown", Severity.WARNING
    if crit is not None and lag_seconds >= crit:
        return "critical", Severity.CRITICAL
    if warn is not None and lag_seconds >= warn:
        return "warning", Severity.WARNING
    return "ok", Severity.INFO


def _postgres_max_timestamp(location: str, column: str) -> datetime | None:
    statement = sql.SQL("SELECT max({col}) AS newest FROM {tbl}").format(
        col=sql.Identifier(column), tbl=qualified_identifier(location)
    )
    with source_connection() as connection:
        row = fetch_one(connection, statement)
    return row["newest"] if row else None


def _parquet_max_timestamp(
    contract: Contract, column: str | None, partition_date: date
) -> tuple[datetime | None, str]:
    """Newest timestamp in a Parquet partition, and which clock it came from."""
    if column:
        frame = read_partition(contract.source.location, partition_date.isoformat())
        if not frame.empty and column in frame.columns:
            series = frame[column].dropna()
            if not series.empty:
                value = series.max()
                stamp = value.to_pydatetime() if hasattr(value, "to_pydatetime") else value
                if stamp.tzinfo is None:
                    stamp = stamp.replace(tzinfo=UTC)
                return stamp, "event_time"

    # Fall back to the object own mtime, and SAY SO. This is strictly weaker
    # evidence: it proves a file was written, not that its contents are recent.
    mtime = object_last_modified(
        partition_prefix(contract.source.location, partition_date.isoformat())
    )
    if mtime is not None:
        return mtime, "object_mtime"
    return None, "unavailable"


def check(
    contract: Contract,
    *,
    partition_date: date,
    reference_time: datetime | None = None,
) -> FreshnessResult:
    """Measure freshness for one dataset."""
    reference = reference_time or datetime.now(UTC)
    warn, crit = contract.freshness.thresholds_seconds()

    if not contract.freshness.enabled:
        return FreshnessResult(
            dataset=contract.dataset,
            timestamp_column=None,
            timestamp_source="unavailable",
            max_timestamp=None,
            reference_time=reference,
            lag_seconds=None,
            warning_after_seconds=warn,
            critical_after_seconds=crit,
            status="unknown",
            severity=Severity.INFO,
            detail="freshness checking is disabled in the contract",
        )

    event_column = contract.source.event_timestamp_column
    ingestion_column = contract.source.ingestion_timestamp_column
    newest: datetime | None = None
    used_column: str | None = None
    source_kind = "unavailable"

    if contract.source.type == "postgres":
        # Try the event time first, then the ingestion time. Each candidate is
        # tried for real rather than assumed present, because an all-NULL
        # column is as useless as an absent one.
        candidates = (
            (event_column, "event_time"),
            (ingestion_column, "ingestion_time"),
        )
        for column, kind in candidates:
            if not column:
                continue
            try:
                found = _postgres_max_timestamp(contract.source.location, column)
            except Exception as exc:
                LOGGER.warning(
                    "%s: cannot read %s for freshness: %s", contract.dataset, column, exc
                )
                continue
            if found is not None:
                newest, used_column, source_kind = found, column, kind
                break
    else:
        newest, source_kind = _parquet_max_timestamp(
            contract, event_column, partition_date
        )
        used_column = event_column if source_kind == "event_time" else None

    if newest is None:
        severity = contract.freshness.on_missing_timestamp
        detail = (
            "no usable timestamp found: the dataset is reported unknown rather "
            "than fresh, because a missing measurement is not a passing one"
        )
        LOGGER.warning("%s: %s", contract.dataset, detail)
        return FreshnessResult(
            dataset=contract.dataset,
            timestamp_column=used_column,
            timestamp_source="unavailable",
            max_timestamp=None,
            reference_time=reference,
            lag_seconds=None,
            warning_after_seconds=warn,
            critical_after_seconds=crit,
            status="unknown",
            severity=severity,
            detail=detail,
        )

    if newest.tzinfo is None:
        newest = newest.replace(tzinfo=UTC)

    lag = int((reference - newest).total_seconds())
    # A negative lag means the newest row is in the future. That is a data
    # problem rather than a freshness one: it is clamped to 0 here and caught
    # by the contract own "not in the future" validity rule.
    lag = max(lag, 0)

    status, severity = _classify(lag, warn, crit)

    detail = (
        f"newest {source_kind}"
        + (f" ({used_column})" if used_column else "")
        + f" is {lag}s old; warning at {warn}s, critical at {crit}s"
    )
    if source_kind == "object_mtime":
        detail += (
            ". Measured from the object mtime because the partition exposes no "
            "usable event timestamp - this proves a file was written, not that "
            "its contents are recent"
        )

    return FreshnessResult(
        dataset=contract.dataset,
        timestamp_column=used_column,
        timestamp_source=source_kind,
        max_timestamp=newest,
        reference_time=reference,
        lag_seconds=lag,
        warning_after_seconds=warn,
        critical_after_seconds=crit,
        status=status,
        severity=severity,
        detail=detail,
    )
