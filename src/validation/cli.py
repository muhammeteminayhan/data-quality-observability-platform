"""Run the contract validations end to end.

    python -m src.validation.cli --date 2024-01-15
    python -m src.validation.cli --date 2024-01-15 --dataset raw.orders

Steps: register the contracts, validate each dataset through GX, evaluate the
engine-owned rules, persist everything against one pipeline run, and report.

Exit code is 1 when any CRITICAL rule failed, so the command is usable as a
gate on its own. A WARNING or ERROR does not fail the command: the gate
decides what blocks, and it is driven by the contracts rather than by this
entrypoint.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, date, datetime

from src.common.db import observability_connection
from src.common.logging import get_logger
from src.common.run_context import new_cli_run, run_scope
from src.common.severity import Severity
from src.contracts import engine as contract_engine
from src.contracts.loader import load_contracts
from src.contracts.registry import register_all
from src.validation.persist import persist
from src.validation.runner import validate_contract

LOGGER = get_logger(__name__)


def run(partition_date: date, *, contracts_dir: str, only: str | None = None) -> dict:
    contracts = load_contracts(contracts_dir)
    if only:
        contracts = [c for c in contracts if c.dataset == only]
        if not contracts:
            raise SystemExit(f"no contract for dataset {only!r}")

    with observability_connection() as connection:
        registered = register_all(connection, contracts)
    LOGGER.info("registered %d dataset(s)", len(registered))

    context = new_cli_run(logical_date=partition_date, notes="validation cli")
    summary: dict = {"run_id": context.run_id_str, "date": partition_date.isoformat(), "datasets": []}

    with run_scope(context):
        for contract in contracts:
            outcome = validate_contract(contract, partition_date=partition_date)
            # Engine-owned rules join the SAME outcome, so the score and the
            # gate see one complete picture per dataset rather than two.
            outcome.outcomes.extend(
                contract_engine.evaluate(contract, partition_date=partition_date)
            )
            if any(not o.success for o in outcome.outcomes):
                outcome.success = False

            validation_run_id = persist(outcome, run_id=context.run_id)
            summary["datasets"].append(
                {
                    "dataset": contract.dataset,
                    "validation_run_id": validation_run_id,
                    "success": outcome.success,
                    "rules": len(outcome.outcomes),
                    "passed": outcome.passed_count,
                    "failed": len(outcome.failed),
                    "max_severity": outcome.max_severity.value,
                    "rows_evaluated": outcome.rows_evaluated,
                    "duration_ms": outcome.duration_ms,
                    "failed_rules": [
                        {
                            "rule": bad.rule_name,
                            "dimension": bad.dimension,
                            "severity": bad.severity.value,
                            "unexpected_count": bad.unexpected_count,
                        }
                        for bad in outcome.failed
                    ],
                }
            )

    summary["total_rules"] = sum(d["rules"] for d in summary["datasets"])
    summary["total_failed"] = sum(d["failed"] for d in summary["datasets"])
    summary["critical_datasets"] = [
        d["dataset"] for d in summary["datasets"] if d["max_severity"] == Severity.CRITICAL.value
    ]
    return summary


def main(argv: list[str] | None = None) -> int:
    from src.common.config import get_settings

    parser = argparse.ArgumentParser(prog="python -m src.validation.cli")
    parser.add_argument("--date", default=None, help="partition date YYYY-MM-DD")
    parser.add_argument("--dataset", default=None, help="validate one dataset only")
    parser.add_argument("--contracts-dir", default=None)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)

    settings = get_settings()
    partition_date = (
        date.fromisoformat(args.date) if args.date else datetime.now(UTC).date()
    )
    summary = run(
        partition_date,
        contracts_dir=args.contracts_dir or str(settings.contracts_dir),
        only=args.dataset,
    )

    if args.json:
        print(json.dumps(summary, indent=2, default=str))
    else:
        print(f"run_id : {summary['run_id']}")
        print(f"date   : {summary['date']}")
        print()
        print(f"{'dataset':<22}{'ok':<7}{'rules':<7}{'failed':<8}{'severity':<10}{'rows':<10}{'ms'}")
        print("-" * 72)
        for item in summary["datasets"]:
            print(
                f"{item['dataset']:<22}{str(item['success']):<7}{item['rules']:<7}"
                f"{item['failed']:<8}{item['max_severity']:<10}"
                f"{str(item['rows_evaluated']):<10}{item['duration_ms']}"
            )
            for bad in item["failed_rules"]:
                print(f"    FAILED {bad['rule']} [{bad['severity']}] unexpected={bad['unexpected_count']}")
        print("-" * 72)
        print(f"rules={summary['total_rules']} failed={summary['total_failed']}")
        if summary["critical_datasets"]:
            print(f"CRITICAL in: {', '.join(summary['critical_datasets'])}")

    return 1 if summary["critical_datasets"] else 0


if __name__ == "__main__":
    sys.exit(main())
