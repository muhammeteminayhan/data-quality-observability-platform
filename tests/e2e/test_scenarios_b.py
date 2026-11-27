"""End-to-end scenarios 6-8: financial, volume and freshness families."""

from __future__ import annotations

import pytest

from tests.e2e.conftest import inject, observations, observe

pytestmark = [pytest.mark.e2e]


# --- 6 --------------------------------------------------------------------
def test_scenario_06_financial_inconsistency_is_caught(clean_source, today):
    """Family 5: totals that no longer reconcile, refunds notwithstanding."""
    inject(["payment_total_mismatch", "line_total_mismatch"], today)

    summary = observe(today)
    seen = observations(summary)

    # The rule excludes refunds and cancelled orders, so a failure here is a
    # genuine mismatch and not the refunds that exist in clean data.
    assert "settled_payments_match_order_total" in seen["raw.orders"].failed_rules
    assert (
        "line_total_equals_quantity_times_price_minus_discount"
        in seen["raw.order_items"].failed_rules
    )
    assert seen["raw.orders"].max_severity == "CRITICAL"
    assert summary["gate"]["decision"] == "FAIL"


# --- 7 --------------------------------------------------------------------
def test_scenario_07_a_volume_spike_is_caught_against_its_own_baseline(clean_source, today):
    """Family 6: the robust median/MAD baseline catches a doubling."""
    # Two clean passes first so a baseline exists. Without history the
    # detector reports `unknown` and claims nothing - correct, but not what
    # this scenario is testing.
    observe(today)
    observe(today)

    results = inject(["volume_spike"], today)
    assert results[0].rows_affected > 0

    summary = observe(today)
    seen = observations(summary)
    assert seen["raw.orders"].volume_status == "critical"

    from src.common.db import fetch_one, observability_connection

    with observability_connection() as connection:
        row = fetch_one(
            connection,
            """
            SELECT v.row_count, v.baseline_median, v.robust_z_score,
                   v.deviation_percent, v.baseline_method, v.history_points,
                   v.severity
            FROM volume_checks v JOIN datasets d USING (dataset_id)
            WHERE d.dataset_name = 'raw.orders'
            ORDER BY v.checked_at DESC LIMIT 1
            """,
        )
    assert row["baseline_method"] == "rolling_median_mad"
    assert row["history_points"] >= 2
    # A doubling is far beyond the contract's critical_z of 6.
    assert abs(float(row["robust_z_score"])) > 6.0
    assert float(row["deviation_percent"]) > 50.0
    assert row["severity"] == "CRITICAL"


def test_scenario_07b_normal_weekend_variation_is_not_an_anomaly(stack):
    """The false alarm the same-day-type baseline exists to prevent."""
    import statistics

    from src.generator.constants import WEEKDAY_VOLUME_WEIGHTS
    from src.observability.volume import MAD_TO_SIGMA, MIN_SPREAD_FRACTION

    def z(count, history):
        median = float(statistics.median(history))
        mad = float(statistics.median([abs(v - median) for v in history]))
        spread = max(mad, median * MIN_SPREAD_FRACTION, 1.0)
        return MAD_TO_SIGMA * (count - median) / spread

    base = 50_000
    weekdays = [round(base * WEEKDAY_VOLUME_WEIGHTS[d]) for d in range(5)]
    weekends = [round(base * WEEKDAY_VOLUME_WEIGHTS[d]) for d in (5, 6)] * 3
    sunday = round(base * WEEKDAY_VOLUME_WEIGHTS[6])

    # Measured against weekdays a perfectly normal Sunday looks catastrophic...
    assert abs(z(sunday, weekdays)) > 6.0
    # ...and against other weekend days it is unremarkable.
    assert abs(z(sunday, weekends)) < 3.5


# --- 8 --------------------------------------------------------------------
def test_scenario_08_stale_data_is_caught_by_the_right_clock(clean_source, today):
    """Family 7: freshness measured from the dataset's own timestamps."""
    inject(["stale_ingestion"], today)

    summary = observe(today)
    seen = observations(summary)
    assert seen["raw.orders"].freshness_status in ("warning", "critical")

    from src.common.db import fetch_one, observability_connection

    with observability_connection() as connection:
        row = fetch_one(
            connection,
            """
            SELECT f.timestamp_source, f.lag_seconds, f.status, f.severity,
                   f.warning_after_seconds, f.critical_after_seconds
            FROM freshness_checks f JOIN datasets d USING (dataset_id)
            WHERE d.dataset_name = 'raw.orders'
            ORDER BY f.checked_at DESC LIMIT 1
            """,
        )
    # The number must say which clock produced it - a lag is meaningless
    # without knowing whether it came from an event time or a file mtime.
    assert row["timestamp_source"] in ("event_time", "ingestion_time")
    assert row["lag_seconds"] >= row["warning_after_seconds"]
