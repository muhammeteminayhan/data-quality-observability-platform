"""Persist every observability measurement into observability_db.

One module so the insert shapes live next to each other and the correlation
key (``run_id``) cannot be forgotten on one of them.

Each writer returns the primary key it created, because the distribution drift
rows reference the profile rows they compared, and the schema change rows
reference the snapshots they diffed. Without those links a dashboard could
show a PSI value with no way to see which two distributions produced it.
"""

from __future__ import annotations

import json
import uuid
from typing import Any

import psycopg

from src.common.db import execute, fetch_one, insert_returning_id
from src.common.logging import get_logger
from src.observability.distribution import ColumnProfile, DriftResult
from src.observability.freshness import FreshnessResult
from src.observability.quality_score import QualityScore
from src.observability.schema_tracker import ObservedSchema, SchemaChange
from src.observability.volume import VolumeResult

LOGGER = get_logger(__name__)


def _json(value: Any) -> str | None:
    return None if value is None else json.dumps(value, default=str)


def save_freshness(
    connection: psycopg.Connection,
    result: FreshnessResult,
    *,
    run_id: uuid.UUID,
    dataset_id: int,
) -> int:
    return int(
        insert_returning_id(
            connection,
            """
            INSERT INTO freshness_checks (
                run_id, dataset_id, timestamp_column, timestamp_source,
                max_timestamp, reference_time, lag_seconds,
                warning_after_seconds, critical_after_seconds,
                status, severity, detail
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING freshness_check_id
            """,
            (
                run_id,
                dataset_id,
                result.timestamp_column,
                result.timestamp_source,
                result.max_timestamp,
                result.reference_time,
                result.lag_seconds,
                result.warning_after_seconds,
                result.critical_after_seconds,
                result.status,
                result.severity.value,
                result.detail,
            ),
        )
    )


def save_volume(
    connection: psycopg.Connection,
    result: VolumeResult,
    *,
    run_id: uuid.UUID,
    dataset_id: int,
) -> int:
    return int(
        insert_returning_id(
            connection,
            """
            INSERT INTO volume_checks (
                run_id, dataset_id, partition_key, row_count, baseline_method,
                history_points, min_history_required, baseline_median,
                baseline_mad, robust_z_score, deviation_percent,
                min_expected_rows, max_expected_rows, weekday, is_weekend,
                status, severity, detail
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s)
            RETURNING volume_check_id
            """,
            (
                run_id,
                dataset_id,
                result.partition_key,
                result.row_count,
                result.baseline_method,
                result.history_points,
                result.min_history_required,
                result.baseline_median,
                result.baseline_mad,
                result.robust_z_score,
                result.deviation_percent,
                result.min_expected_rows,
                result.max_expected_rows,
                result.weekday,
                result.is_weekend,
                result.status,
                result.severity.value,
                result.detail,
            ),
        )
    )


def save_schema_snapshot(
    connection: psycopg.Connection,
    snapshot: ObservedSchema,
    *,
    run_id: uuid.UUID,
    dataset_id: int,
) -> int:
    return int(
        insert_returning_id(
            connection,
            """
            INSERT INTO schema_snapshots (
                run_id, dataset_id, fingerprint, column_count, columns
            )
            VALUES (%s, %s, %s, %s, %s)
            RETURNING schema_snapshot_id
            """,
            (
                run_id,
                dataset_id,
                snapshot.fingerprint,
                len(snapshot.columns),
                _json(snapshot.as_json()),
            ),
        )
    )


def latest_schema_snapshot(
    connection: psycopg.Connection, dataset_id: int, *, before_run_id: uuid.UUID | None = None
) -> dict[str, Any] | None:
    """The most recent snapshot, optionally excluding the current run.

    Excluding the current run matters: the snapshot for this run is written
    before the diff is computed, so without the filter a dataset would always
    be diffed against itself and no change could ever be detected.
    """
    if before_run_id is None:
        return fetch_one(
            connection,
            """
            SELECT * FROM schema_snapshots
            WHERE dataset_id = %s
            ORDER BY captured_at DESC, schema_snapshot_id DESC
            LIMIT 1
            """,
            (dataset_id,),
        )
    return fetch_one(
        connection,
        """
        SELECT * FROM schema_snapshots
        WHERE dataset_id = %s AND run_id <> %s
        ORDER BY captured_at DESC, schema_snapshot_id DESC
        LIMIT 1
        """,
        (dataset_id, before_run_id),
    )


def save_schema_change(
    connection: psycopg.Connection,
    change: SchemaChange,
    *,
    run_id: uuid.UUID,
    dataset_id: int,
    from_snapshot_id: int | None,
    to_snapshot_id: int,
) -> int:
    return int(
        insert_returning_id(
            connection,
            """
            INSERT INTO schema_changes (
                run_id, dataset_id, from_snapshot_id, to_snapshot_id,
                change_type, column_name, previous_column, previous_type,
                new_type, confidence, is_confirmed, severity, detail
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING schema_change_id
            """,
            (
                run_id,
                dataset_id,
                from_snapshot_id,
                to_snapshot_id,
                change.change_type,
                change.column_name,
                change.previous_column,
                change.previous_type,
                change.new_type,
                change.confidence,
                change.is_confirmed,
                change.severity.value,
                change.detail,
            ),
        )
    )


def save_distribution_profile(
    connection: psycopg.Connection,
    profile: ColumnProfile,
    *,
    run_id: uuid.UUID,
    dataset_id: int,
) -> int:
    return int(
        insert_returning_id(
            connection,
            """
            INSERT INTO distribution_profiles (
                run_id, dataset_id, column_name, column_kind, sample_size,
                null_count, distinct_count, mean_value, stddev_value,
                min_value, p25_value, median_value, p75_value, max_value,
                bin_edges, bin_counts, category_counts
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s)
            RETURNING distribution_profile_id
            """,
            (
                run_id,
                dataset_id,
                profile.column,
                profile.kind,
                profile.sample_size,
                profile.null_count,
                profile.distinct_count,
                profile.mean,
                profile.stddev,
                profile.minimum,
                profile.p25,
                profile.median,
                profile.p75,
                profile.maximum,
                _json(profile.bin_edges),
                _json(profile.bin_counts),
                _json(profile.category_counts),
            ),
        )
    )


def latest_distribution_profile(
    connection: psycopg.Connection,
    dataset_id: int,
    column: str,
    *,
    before_run_id: uuid.UUID | None = None,
) -> dict[str, Any] | None:
    """The reference profile for a drift comparison.

    Excludes the current run for the same reason as the schema snapshot: the
    current profile is written first, so without the filter every column would
    be compared against itself and PSI would always be exactly 0.
    """
    clause = "AND run_id <> %s" if before_run_id else ""
    params: list[Any] = [dataset_id, column]
    if before_run_id:
        params.append(before_run_id)
    return fetch_one(
        connection,
        f"""
        SELECT * FROM distribution_profiles
        WHERE dataset_id = %s AND column_name = %s {clause}
        ORDER BY captured_at DESC, distribution_profile_id DESC
        LIMIT 1
        """,
        tuple(params),
    )


def profile_from_row(row: dict[str, Any], dataset: str) -> ColumnProfile:
    """Rebuild a ColumnProfile from a stored row.

    Needed because a drift comparison happens between a profile measured now
    and one measured on an earlier run, which exists only in the database.
    """
    return ColumnProfile(
        dataset=dataset,
        column=row["column_name"],
        kind=row["column_kind"],
        sample_size=int(row["sample_size"]),
        null_count=int(row["null_count"]),
        distinct_count=row["distinct_count"],
        mean=float(row["mean_value"]) if row["mean_value"] is not None else None,
        stddev=float(row["stddev_value"]) if row["stddev_value"] is not None else None,
        minimum=float(row["min_value"]) if row["min_value"] is not None else None,
        p25=float(row["p25_value"]) if row["p25_value"] is not None else None,
        median=float(row["median_value"]) if row["median_value"] is not None else None,
        p75=float(row["p75_value"]) if row["p75_value"] is not None else None,
        maximum=float(row["max_value"]) if row["max_value"] is not None else None,
        bin_edges=row["bin_edges"],
        bin_counts=row["bin_counts"],
        category_counts=row["category_counts"],
    )


def save_distribution_drift(
    connection: psycopg.Connection,
    result: DriftResult,
    *,
    run_id: uuid.UUID,
    dataset_id: int,
    reference_profile_id: int | None,
    current_profile_id: int,
) -> int:
    return int(
        insert_returning_id(
            connection,
            """
            INSERT INTO distribution_drift (
                run_id, dataset_id, column_name, column_kind, method,
                reference_profile_id, current_profile_id,
                reference_sample_size, current_sample_size, min_sample_size,
                statistic, p_value, degrees_of_freedom,
                threshold_warning, threshold_critical,
                status, severity, is_significant, detail
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s)
            RETURNING distribution_drift_id
            """,
            (
                run_id,
                dataset_id,
                result.column,
                result.kind,
                result.method,
                reference_profile_id,
                current_profile_id,
                result.reference_sample_size,
                result.current_sample_size,
                result.min_sample_size,
                result.statistic,
                result.p_value,
                result.degrees_of_freedom,
                result.threshold_warning,
                result.threshold_critical,
                result.status,
                result.severity.value,
                result.is_significant,
                result.detail,
            ),
        )
    )


def save_quality_score(
    connection: psycopg.Connection,
    score: QualityScore,
    *,
    run_id: uuid.UUID,
    dataset_id: int,
) -> int:
    return int(
        insert_returning_id(
            connection,
            """
            INSERT INTO quality_scores (
                run_id, dataset_id, overall_score,
                completeness_score, uniqueness_score, validity_score,
                consistency_score, freshness_score, volume_score,
                dimensions_measured, dimensions_total, weights_used,
                unmeasured_dimensions, has_critical_failure, health_status
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING quality_score_id
            """,
            (
                run_id,
                dataset_id,
                score.overall_score,
                score.dimension_scores.get("completeness"),
                score.dimension_scores.get("uniqueness"),
                score.dimension_scores.get("validity"),
                score.dimension_scores.get("consistency"),
                score.dimension_scores.get("freshness"),
                score.dimension_scores.get("volume"),
                score.dimensions_measured,
                score.dimensions_total,
                _json(score.weights_used),
                _json(score.unmeasured_dimensions),
                score.has_critical_failure,
                score.health_status,
            ),
        )
    )


def save_metric(
    connection: psycopg.Connection,
    *,
    run_id: uuid.UUID | None,
    dataset_id: int | None,
    metric_name: str,
    metric_value: float,
    unit: str | None = None,
    labels: dict[str, Any] | None = None,
) -> None:
    """Append a business-level metric to the durable history.

    Prometheus keeps the operational view; anything that has to be queryable
    months later, or joined to a dataset or an incident, lands here.
    """
    execute(
        connection,
        """
        INSERT INTO metric_history (
            run_id, dataset_id, metric_name, metric_value, unit, labels
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (run_id, dataset_id, metric_name, metric_value, unit, _json(labels or {})),
    )
