"""Run the full observability pass for one partition.

    python -m src.observability.cli --date 2024-01-15
    python -m src.observability.cli --date 2024-01-15 --no-gate

Order matters and is deliberate:

  1. register the contracts                (datasets, contract_versions)
  2. validate                              (GX + the contract engine)
  3. freshness, volume                     (per dataset)
  4. schema snapshot, then diff            (snapshot written first, diffed
                                            against the PREVIOUS run)
  5. distribution profiles, then drift     (same pattern)
  6. quality score                         (needs everything above)
  7. reconcile incidents                   (findings + verified healthy checks)
  8. quality gate                          (raises on FAIL)

The gate runs last because it has to see every signal. `--no-gate` skips only
the raise, not the recording, which is what the Airflow DAG uses for the
observability task before its separate gate task.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from typing import Any

from src.common.db import observability_connection
from src.common.logging import get_logger
from src.common.run_context import RunContext, ensure_run, finish_run, new_cli_run
from src.common.severity import Severity, max_severity
from src.contracts import engine as contract_engine
from src.contracts.loader import load_contracts
from src.contracts.models import Contract
from src.contracts.registry import register_all
from src.gates import gate as quality_gate
from src.observability import (
    anomaly,
    distribution,
    freshness,
    persist,
    quality_score,
    schema_tracker,
    volume,
)
from src.validation.persist import persist as persist_validation
from src.validation.runner import validate_contract

LOGGER = get_logger(__name__)


@dataclass
class DatasetReport:
    dataset: str
    validation_run_id: int | None = None
    rules: int = 0
    rules_failed: int = 0
    freshness_status: str = "n/a"
    volume_status: str = "n/a"
    schema_changes: int = 0
    drift_findings: int = 0
    score: float | None = None
    health: str = "unknown"
    severities: list[Severity] = field(default_factory=list)
    failed_rules: list[str] = field(default_factory=list)
    incidents_opened: int = 0
    incidents_resolved: int = 0

    @property
    def max_severity(self) -> Severity:
        return max_severity(self.severities)


def _run_drift(
    connection,
    contract: Contract,
    *,
    partition_date: date,
    run_id,
    dataset_id: int,
) -> list[distribution.DriftResult]:
    """Profile the drift columns and compare each against its reference."""
    results: list[distribution.DriftResult] = []
    spec = contract.distribution
    if not spec.enabled:
        return results

    # Numeric: re-bin the current data using the REFERENCE edges, so both
    # sides are counted into the same buckets.
    for item in spec.numeric:
        reference_row = persist.latest_distribution_profile(
            connection, dataset_id, item.column, before_run_id=run_id
        )
        reference = (
            persist.profile_from_row(reference_row, contract.dataset)
            if reference_row
            else None
        )
        if reference is not None and reference.bin_edges:
            current = distribution.rebin_against_reference(
                contract, partition_date, reference, item
            )
        else:
            built = distribution.build_profiles(contract, partition_date)
            current = next(
                (p for p in built if p.column == item.column and p.kind == "numeric"),
                None,
            )
        if current is None:
            continue
        current_id = persist.save_distribution_profile(
            connection, current, run_id=run_id, dataset_id=dataset_id
        )
        result = distribution.compare_numeric(reference, current, item, spec.min_sample_size)
        persist.save_distribution_drift(
            connection,
            result,
            run_id=run_id,
            dataset_id=dataset_id,
            reference_profile_id=int(reference_row["distribution_profile_id"])
            if reference_row
            else None,
            current_profile_id=current_id,
        )
        results.append(result)

    # Categorical.
    built = distribution.build_profiles(contract, partition_date)
    for item in spec.categorical:
        current = next(
            (p for p in built if p.column == item.column and p.kind == "categorical"),
            None,
        )
        if current is None:
            continue
        reference_row = persist.latest_distribution_profile(
            connection, dataset_id, item.column, before_run_id=run_id
        )
        reference = (
            persist.profile_from_row(reference_row, contract.dataset)
            if reference_row
            else None
        )
        current_id = persist.save_distribution_profile(
            connection, current, run_id=run_id, dataset_id=dataset_id
        )
        result = distribution.compare_categorical(
            reference, current, item, spec.min_sample_size
        )
        persist.save_distribution_drift(
            connection,
            result,
            run_id=run_id,
            dataset_id=dataset_id,
            reference_profile_id=int(reference_row["distribution_profile_id"])
            if reference_row
            else None,
            current_profile_id=current_id,
        )
        results.append(result)

    return results


def process_dataset(
    connection,
    contract: Contract,
    *,
    partition_date: date,
    run_id,
    dataset_id: int,
    reference_time: datetime | None = None,
) -> DatasetReport:
    """Run every detector for one dataset and reconcile its incidents."""
    report = DatasetReport(dataset=contract.dataset)

    # --- 2. validation ---------------------------------------------------
    outcome = validate_contract(contract, partition_date=partition_date)
    outcome.outcomes.extend(
        contract_engine.evaluate(contract, partition_date=partition_date)
    )
    if any(not o.success for o in outcome.outcomes):
        outcome.success = False
    report.validation_run_id = persist_validation(outcome, run_id=run_id)
    report.rules = len(outcome.outcomes)
    report.rules_failed = len(outcome.failed)
    report.failed_rules = [bad.rule_name for bad in outcome.failed]
    report.severities.append(outcome.max_severity)

    # --- 3. freshness and volume ----------------------------------------
    freshness_result = freshness.check(
        contract, partition_date=partition_date, reference_time=reference_time
    )
    persist.save_freshness(
        connection, freshness_result, run_id=run_id, dataset_id=dataset_id
    )
    report.freshness_status = freshness_result.status
    if freshness_result.status != "ok":
        report.severities.append(freshness_result.severity)

    volume_result = volume.check(
        contract,
        partition_date=partition_date,
        dataset_id=dataset_id,
        exclude_run_id=run_id,
    )
    persist.save_volume(connection, volume_result, run_id=run_id, dataset_id=dataset_id)
    report.volume_status = volume_result.status
    if volume_result.status not in ("ok", "unknown"):
        report.severities.append(volume_result.severity)

    # --- 4. schema -------------------------------------------------------
    previous_row = persist.latest_schema_snapshot(
        connection, dataset_id, before_run_id=run_id
    )
    current_snapshot = schema_tracker.snapshot(contract, partition_date)
    current_snapshot_id = persist.save_schema_snapshot(
        connection, current_snapshot, run_id=run_id, dataset_id=dataset_id
    )
    previous_snapshot = None
    if previous_row:
        previous_snapshot = schema_tracker.ObservedSchema(
            dataset=contract.dataset,
            columns=tuple(
                schema_tracker.ObservedColumn(
                    name=column["name"],
                    type=column["type"],
                    nullable=bool(column["nullable"]),
                    position=int(column["position"]),
                )
                for column in previous_row["columns"]
            ),
        )

    schema_changes = schema_tracker.classify(previous_snapshot, current_snapshot, contract)
    for change in schema_changes:
        persist.save_schema_change(
            connection,
            change,
            run_id=run_id,
            dataset_id=dataset_id,
            from_snapshot_id=int(previous_row["schema_snapshot_id"]) if previous_row else None,
            to_snapshot_id=current_snapshot_id,
        )
        if change.severity > Severity.INFO:
            report.severities.append(change.severity)
    report.schema_changes = len(schema_changes)

    # --- 5. distribution -------------------------------------------------
    drift_results = _run_drift(
        connection,
        contract,
        partition_date=partition_date,
        run_id=run_id,
        dataset_id=dataset_id,
    )
    significant = [d for d in drift_results if d.is_significant]
    report.drift_findings = len(significant)
    for item in significant:
        report.severities.append(item.severity)

    # --- 6. score --------------------------------------------------------
    score = quality_score.compute(
        contract,
        rule_outcomes=outcome.outcomes,
        freshness_result=freshness_result,
        volume_result=volume_result,
        schema_changes=schema_changes,
    )
    persist.save_quality_score(connection, score, run_id=run_id, dataset_id=dataset_id)
    persist.save_metric(
        connection,
        run_id=run_id,
        dataset_id=dataset_id,
        metric_name="quality_score",
        metric_value=score.overall_score,
        unit="score",
        labels={"health": score.health_status},
    )
    report.score = score.overall_score
    report.health = score.health_status

    # --- 7. incidents ----------------------------------------------------
    reconciliation = anomaly.reconcile(
        connection,
        contract,
        run_id=run_id,
        dataset_id=dataset_id,
        rule_outcomes=outcome.outcomes,
        freshness_result=freshness_result,
        volume_result=volume_result,
        schema_changes=schema_changes,
        drift_results=drift_results,
        score=score,
    )
    report.incidents_opened = len(
        [a for a in reconciliation.actions if a.action == "OPENED"]
    )
    report.incidents_resolved = len(reconciliation.resolved)

    return report


def run(
    partition_date: date,
    *,
    contracts_dir: str,
    enforce_gate: bool = True,
    context: RunContext | None = None,
    reference_time: datetime | None = None,
) -> dict[str, Any]:
    """Full observability pass. Returns a JSON-serialisable summary."""
    contracts = load_contracts(contracts_dir)

    run_context = context or new_cli_run(
        logical_date=partition_date, notes="observability cli"
    )
    ensure_run(run_context)

    reports: list[DatasetReport] = []
    try:
        with observability_connection() as connection:
            registered = register_all(connection, contracts)

        for contract in contracts:
            dataset_id = registered[contract.dataset][0]
            # A fresh connection per dataset keeps one dataset's failure from
            # rolling back another's measurements.
            with observability_connection() as connection:
                reports.append(
                    process_dataset(
                        connection,
                        contract,
                        partition_date=partition_date,
                        run_id=run_context.run_id,
                        dataset_id=dataset_id,
                        reference_time=reference_time,
                    )
                )
    except Exception:
        finish_run(run_context.run_id, "failed", notes="observability pass raised")
        raise

    signals = [
        quality_gate.signal_from_parts(
            report.dataset,
            report.severities,
            failed_rules=report.failed_rules,
            detail=(
                f"score={report.score} health={report.health} "
                f"freshness={report.freshness_status} volume={report.volume_status}"
            ),
        )
        for report in reports
    ]

    summary: dict[str, Any] = {
        "run_id": run_context.run_id_str,
        "date": partition_date.isoformat(),
        "datasets": [
            {
                "dataset": r.dataset,
                "rules": r.rules,
                "rules_failed": r.rules_failed,
                "freshness": r.freshness_status,
                "volume": r.volume_status,
                "schema_changes": r.schema_changes,
                "drift_findings": r.drift_findings,
                "score": r.score,
                "health": r.health,
                "max_severity": r.max_severity.value,
                "incidents_opened": r.incidents_opened,
                "incidents_resolved": r.incidents_resolved,
            }
            for r in reports
        ],
    }

    if enforce_gate:
        try:
            decision = quality_gate.enforce(signals, run_id=run_context.run_id)
            summary["gate"] = {"decision": decision.decision, "reason": decision.reason}
            finish_run(run_context.run_id, "success")
        except quality_gate.QualityGateFailed as failure:
            summary["gate"] = {
                "decision": failure.decision.decision,
                "reason": failure.decision.reason,
            }
            # `blocked` rather than `failed`: the platform worked correctly and
            # stopped the pipeline on purpose. Telling the two apart matters
            # when reading a dashboard.
            finish_run(run_context.run_id, "blocked", notes=failure.decision.reason)
            summary["gate_failed"] = True
            return summary
    else:
        decision = quality_gate.evaluate(signals)
        quality_gate.record(decision, run_id=run_context.run_id)
        summary["gate"] = {"decision": decision.decision, "reason": decision.reason}
        finish_run(run_context.run_id, "success")

    return summary


def main(argv: list[str] | None = None) -> int:
    from src.common.config import get_settings

    parser = argparse.ArgumentParser(prog="python -m src.observability.cli")
    parser.add_argument("--date", default=None, help="partition date YYYY-MM-DD")
    parser.add_argument("--contracts-dir", default=None)
    parser.add_argument(
        "--no-gate",
        action="store_true",
        help="record the gate decision but do not raise on FAIL",
    )
    parser.add_argument(
        "--reference-time",
        default=None,
        help="ISO instant to measure freshness against (default: now)",
    )
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)

    settings = get_settings()
    partition_date = (
        date.fromisoformat(args.date) if args.date else datetime.now(UTC).date()
    )
    reference_time = (
        datetime.fromisoformat(args.reference_time) if args.reference_time else None
    )

    summary = run(
        partition_date,
        contracts_dir=args.contracts_dir or str(settings.contracts_dir),
        enforce_gate=not args.no_gate,
        reference_time=reference_time,
    )

    if args.json:
        print(json.dumps(summary, indent=2, default=str))
    else:
        print(f"run_id : {summary['run_id']}")
        print(f"date   : {summary['date']}")
        print()
        header = (
            f"{'dataset':<22}{'score':<8}{'health':<11}{'fresh':<10}"
            f"{'volume':<14}{'rules':<7}{'fail':<6}{'schema':<8}{'drift':<7}{'sev'}"
        )
        print(header)
        print("-" * len(header))
        for item in summary["datasets"]:
            print(
                f"{item['dataset']:<22}{str(item['score']):<8}{item['health']:<11}"
                f"{item['freshness']:<10}{item['volume']:<14}{item['rules']:<7}"
                f"{item['rules_failed']:<6}{item['schema_changes']:<8}"
                f"{item['drift_findings']:<7}{item['max_severity']}"
            )
        print("-" * len(header))
        gate = summary.get("gate", {})
        print(f"gate   : {gate.get('decision')} - {gate.get('reason')}")
        opened = sum(d["incidents_opened"] for d in summary["datasets"])
        resolved = sum(d["incidents_resolved"] for d in summary["datasets"])
        print(f"incidents opened={opened} resolved={resolved}")

    return 1 if summary.get("gate_failed") else 0


if __name__ == "__main__":
    sys.exit(main())
