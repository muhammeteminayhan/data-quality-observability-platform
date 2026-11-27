"""Volume: is the row count what history says it should be.

Why a robust baseline
---------------------
A mean and a standard deviation are both dragged around by the very outliers
this check exists to find: one 10x spike inflates the standard deviation so
much that the next genuine spike looks normal. So the baseline is a rolling
MEDIAN with a Median Absolute Deviation (MAD) spread, and the test statistic
is a robust z-score:

    z = 0.6745 * (count - median) / MAD

The 0.6745 factor makes MAD comparable to a standard deviation for normally
distributed data, so the contract thresholds (warning_z 3.5, critical_z 6.0)
read on the familiar scale.

Why same-day-type comparison
----------------------------
Order volume on this platform genuinely drops to about 55-62 percent at
weekends - that is in the generated data on purpose. Comparing a Sunday
against a window that is mostly weekdays would report a real 40 percent drop
every single weekend. With `same_day_type: true` the baseline for a Saturday
is built only from other Saturdays and Sundays, so normal weekend behaviour
produces a z-score near zero.

Why a minimum history
---------------------
A median of two points is not a baseline. Below `min_history_points`
comparable observations the check reports status `unknown` with method
`insufficient_history` and does NOT emit a threshold. Inventing a bound from
one prior run is how a volume monitor starts crying wolf on day two.
"""

from __future__ import annotations

import statistics
from dataclasses import dataclass
from datetime import date

from psycopg import sql

from src.common.db import (
    fetch_all,
    fetch_value,
    observability_connection,
    qualified_identifier,
    source_connection,
)
from src.common.logging import get_logger
from src.common.severity import Severity
from src.common.storage import list_objects, partition_prefix, read_partition
from src.contracts.models import Contract

LOGGER = get_logger(__name__)

# Scale factor making MAD comparable with a standard deviation.
MAD_TO_SIGMA = 0.6745

# When every historical observation is identical, MAD is 0 and the z-score is
# undefined. A floor of 1 percent of the median keeps the statistic finite and
# means an exactly-stable dataset needs a >1 percent move before it scores at
# all - which is the intended behaviour for something that never varies.
MIN_SPREAD_FRACTION = 0.01


@dataclass
class VolumeResult:
    dataset: str
    partition_key: str | None
    row_count: int
    baseline_method: str
    history_points: int
    min_history_required: int
    baseline_median: float | None
    baseline_mad: float | None
    robust_z_score: float | None
    deviation_percent: float | None
    min_expected_rows: int | None
    max_expected_rows: int | None
    weekday: int
    is_weekend: bool
    status: str
    severity: Severity
    detail: str


def current_row_count(contract: Contract, partition_date: date) -> int:
    """Row count of the dataset as it is right now."""
    if contract.source.type == "postgres":
        statement = sql.SQL("SELECT count(*) FROM {tbl}").format(
            tbl=qualified_identifier(contract.source.location)
        )
        with source_connection() as connection:
            return int(fetch_value(connection, statement) or 0)

    prefix = partition_prefix(contract.source.location, partition_date.isoformat())
    objects = [o for o in list_objects(prefix) if o.key.endswith(".parquet")]
    if not objects:
        # An absent partition is zero rows, which is exactly the signal a
        # volume check should raise - not an exception.
        LOGGER.warning("no parquet objects under %s; row count is 0", prefix)
        return 0
    return int(len(read_partition(contract.source.location, partition_date.isoformat())))


def load_history(
    dataset_id: int,
    *,
    window_days: int,
    same_day_type: bool,
    is_weekend: bool,
    exclude_run_id=None,
) -> list[int]:
    """Comparable historical row counts, newest first.

    Only observations the platform actually recorded are used. Anomalous
    observations are NOT filtered out: a median is robust enough to tolerate
    them, and silently dropping them would let a slow drift redefine normal.
    """
    # NOTE: rows with status='unknown' are deliberately INCLUDED.
    #
    # They were excluded at first, to keep "bad" observations out of the
    # baseline. That created a deadlock: the very first check has no history
    # so it reports unknown, and if unknown rows are not history, the next
    # check has no history either - for ever. Measured: 104 consecutive
    # checks, every one `insufficient_history`, with the baseline never once
    # computable.
    #
    # The fix follows from what the columns mean. `row_count` is a real
    # measurement of the dataset taken at that moment; `status` records
    # whether the check could CONCLUDE anything from it. Those are different
    # facts, and only the first one belongs in a baseline.
    clauses = [
        "dataset_id = %s",
        "checked_at >= now() - make_interval(days => %s)",
    ]
    params: list = [dataset_id, window_days]

    if same_day_type:
        clauses.append("is_weekend = %s")
        params.append(is_weekend)
    if exclude_run_id is not None:
        clauses.append("run_id <> %s")
        params.append(exclude_run_id)

    statement = f"""
        SELECT row_count
        FROM volume_checks
        WHERE {' AND '.join(clauses)}
        ORDER BY checked_at DESC
        LIMIT 400
    """
    with observability_connection() as connection:
        rows = fetch_all(connection, statement, tuple(params))
    return [int(row["row_count"]) for row in rows]


def _absolute_bounds_status(
    count: int, contract: Contract
) -> tuple[str, Severity, str] | None:
    """Hard min/max from the contract.

    Applied regardless of history, because some bounds are definitional rather
    than statistical: a product catalogue with 3 rows is broken no matter what
    last week looked like.
    """
    low = contract.volume.min_rows
    high = contract.volume.max_rows
    if low is not None and count < low:
        return (
            "critical",
            Severity.CRITICAL,
            f"row count {count} is below the contract minimum of {low}",
        )
    if high is not None and count > high:
        return (
            "critical",
            Severity.CRITICAL,
            f"row count {count} is above the contract maximum of {high}",
        )
    return None


def check(
    contract: Contract,
    *,
    partition_date: date,
    dataset_id: int,
    exclude_run_id=None,
) -> VolumeResult:
    """Measure volume for one dataset against its own history."""
    baseline = contract.volume.baseline
    weekday = partition_date.weekday()
    is_weekend = weekday >= 5
    count = current_row_count(contract, partition_date)

    result = VolumeResult(
        dataset=contract.dataset,
        partition_key=partition_date.isoformat(),
        row_count=count,
        baseline_method="insufficient_history",
        history_points=0,
        min_history_required=baseline.min_history_points,
        baseline_median=None,
        baseline_mad=None,
        robust_z_score=None,
        deviation_percent=None,
        min_expected_rows=None,
        max_expected_rows=None,
        weekday=weekday,
        is_weekend=is_weekend,
        status="unknown",
        severity=Severity.INFO,
        detail="",
    )

    if not contract.volume.enabled:
        result.detail = "volume checking is disabled in the contract"
        return result

    # Hard bounds first: they need no history and they outrank the statistics.
    absolute = _absolute_bounds_status(count, contract)
    if absolute is not None:
        status, severity, detail = absolute
        result.baseline_method = "absolute_bounds"
        result.min_expected_rows = contract.volume.min_rows
        result.max_expected_rows = contract.volume.max_rows
        result.status = status
        result.severity = severity
        result.detail = detail
        return result

    history = load_history(
        dataset_id,
        window_days=baseline.window_days,
        same_day_type=baseline.same_day_type,
        is_weekend=is_weekend,
        exclude_run_id=exclude_run_id,
    )
    result.history_points = len(history)

    if len(history) < baseline.min_history_points:
        result.status = "unknown"
        result.severity = Severity.INFO
        result.baseline_method = "insufficient_history"
        comparable = (
            f"{'weekend' if is_weekend else 'weekday'} observations"
            if baseline.same_day_type
            else "observations"
        )
        result.detail = (
            f"only {len(history)} comparable {comparable} available, "
            f"{baseline.min_history_points} required; no threshold is claimed"
        )
        LOGGER.info("%s: %s", contract.dataset, result.detail)
        return result

    median = float(statistics.median(history))
    deviations = [abs(value - median) for value in history]
    mad = float(statistics.median(deviations))
    spread = max(mad, median * MIN_SPREAD_FRACTION, 1.0)

    z = MAD_TO_SIGMA * (count - median) / spread
    deviation_percent = ((count - median) / median * 100.0) if median else 0.0

    warn_z = baseline.warning_z
    crit_z = baseline.critical_z
    # Expected band, reported so a dashboard can draw it.
    result.min_expected_rows = int(median - crit_z * spread / MAD_TO_SIGMA)
    result.max_expected_rows = int(median + crit_z * spread / MAD_TO_SIGMA)

    if abs(z) >= crit_z:
        status, severity = "critical", Severity.CRITICAL
    elif abs(z) >= warn_z:
        status, severity = "warning", contract.volume.severity
    else:
        status, severity = "ok", Severity.INFO

    result.baseline_method = "rolling_median_mad"
    result.baseline_median = round(median, 4)
    result.baseline_mad = round(mad, 4)
    result.robust_z_score = round(z, 4)
    result.deviation_percent = round(deviation_percent, 4)
    result.status = status
    result.severity = severity
    result.detail = (
        f"count {count} vs median {median:.0f} over {len(history)} "
        f"{'same-day-type ' if baseline.same_day_type else ''}points "
        f"(MAD {mad:.1f}, robust z {z:.2f}, {deviation_percent:+.1f}%); "
        f"warning at |z|>={warn_z}, critical at |z|>={crit_z}"
    )
    return result
