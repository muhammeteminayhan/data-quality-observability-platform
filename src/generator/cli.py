"""Command line entrypoint for the data generator.

    python -m src.generator.cli --seed 20240115 --records 50000 \
        --date 2024-01-15 --mode clean --output both

    python -m src.generator.cli --mode fault --fault-profile volume_drop

Flags (section 4 of the specification):

    --seed           deterministic seed; the same seed reproduces the dataset
    --records        number of orders; the other tables scale proportionally
    --date           partition date; every timestamp derives from it
    --mode           clean | fault
    --fault-profile  which faults to inject when --mode fault
    --output         postgres | parquet | both | local
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, date, datetime

from src.common.config import get_settings
from src.common.db import source_connection
from src.common.logging import get_logger
from src.common.run_context import new_cli_run, run_scope
from src.generator import parquet_sink, postgres_sink
from src.generator.constants import DEFAULT_VOLUMES, WEEKDAY_VOLUME_WEIGHTS
from src.generator.fault_injector import inject, resolve_profile
from src.generator.records import generate

LOGGER = get_logger(__name__)


def scaled_volumes(orders: int, *, partition_date: date, apply_weekday: bool = True) -> dict[str, int]:
    """Scale every table from the requested order count.

    The ratios come from DEFAULT_VOLUMES, so ``--records 5000`` produces a
    proportionally smaller but structurally identical dataset.

    The weekday weighting is applied here rather than in the detector, because
    it has to be a property of the DATA: a Sunday genuinely has fewer orders.
    The volume module then has to learn that from history instead of being
    told, which is what makes "do not flag normal weekend variation" a real
    test rather than a special case in the checker.
    """
    base_orders = DEFAULT_VOLUMES["orders"]
    ratio = orders / base_orders if base_orders else 1.0

    weight = (
        WEEKDAY_VOLUME_WEIGHTS[partition_date.weekday()] if apply_weekday else 1.0
    )

    volumes = {
        "customers": max(1, round(DEFAULT_VOLUMES["customers"] * ratio)),
        "products": max(1, round(DEFAULT_VOLUMES["products"] * ratio)),
        "orders": max(1, round(orders * weight)),
        "order_items": max(1, round(DEFAULT_VOLUMES["order_items"] * ratio * weight)),
        "payments": max(1, round(DEFAULT_VOLUMES["payments"] * ratio * weight)),
    }
    return volumes


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m src.generator.cli",
        description="Generate deterministic synthetic commerce data.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="deterministic seed (default: DQ_SEED from the environment)",
    )
    parser.add_argument(
        "--records",
        type=int,
        default=DEFAULT_VOLUMES["orders"],
        help=f"number of orders to generate (default {DEFAULT_VOLUMES['orders']})",
    )
    parser.add_argument(
        "--date",
        default=None,
        help="partition date YYYY-MM-DD (default: today, UTC)",
    )
    parser.add_argument(
        "--mode",
        choices=("clean", "fault"),
        default="clean",
        help="clean data, or data followed by fault injection",
    )
    parser.add_argument(
        "--fault-profile",
        default="clean",
        help="fault profile or scenario name, used when --mode fault",
    )
    parser.add_argument(
        "--output",
        choices=("postgres", "parquet", "both", "local"),
        default="both",
        help="where to write the generated data",
    )
    parser.add_argument(
        "--local-dir",
        default=None,
        help="target directory for --output local (default <DQ_HOME>/data/parquet)",
    )
    parser.add_argument(
        "--truncate",
        action="store_true",
        help="empty the raw tables before loading (fresh baseline)",
    )
    parser.add_argument(
        "--no-weekday-weighting",
        action="store_true",
        help="ignore the weekday volume curve and generate the exact count",
    )
    parser.add_argument(
        "--batch-id",
        default=None,
        help="override the batch id (default: generated from date and seed)",
    )
    parser.add_argument("--json", action="store_true", help="print a JSON summary")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    settings = get_settings()

    seed = args.seed if args.seed is not None else settings.seed
    partition_date = (
        date.fromisoformat(args.date) if args.date else datetime.now(UTC).date()
    )
    batch_id = args.batch_id or f"{partition_date.isoformat()}-{seed}"
    volumes = scaled_volumes(
        args.records,
        partition_date=partition_date,
        apply_weekday=not args.no_weekday_weighting,
    )

    LOGGER.info(
        "generating seed=%s date=%s batch=%s volumes=%s",
        seed,
        partition_date,
        batch_id,
        volumes,
    )

    run = new_cli_run(
        logical_date=partition_date,
        fault_profile=args.fault_profile if args.mode == "fault" else "clean",
        notes=f"generator mode={args.mode} output={args.output}",
    )

    summary: dict[str, object] = {
        "run_id": run.run_id_str,
        "seed": seed,
        "date": partition_date.isoformat(),
        "batch_id": batch_id,
        "mode": args.mode,
        "output": args.output,
        "volumes_requested": volumes,
    }

    with run_scope(run):
        data = generate(
            seed=seed,
            partition_date=partition_date,
            batch_id=batch_id,
            volumes=volumes,
        )
        summary["row_counts"] = data.row_counts()
        summary["fingerprint"] = data.fingerprint()
        LOGGER.info("generated %s", data.row_counts())

        if args.output in ("postgres", "both"):
            summary["postgres_written"] = postgres_sink.write(
                data, truncate=args.truncate
            )

        if args.output in ("parquet", "both"):
            summary["parquet_written"] = parquet_sink.write(data)

        if args.output == "local":
            target = args.local_dir or str(settings.home / "data" / "parquet")
            summary["local_written"] = parquet_sink.write_local(data, target)

        if args.mode == "fault":
            scenario_names = list(resolve_profile(args.fault_profile))
            if not scenario_names:
                LOGGER.warning(
                    "--mode fault with profile %r injects nothing", args.fault_profile
                )
                summary["faults"] = []
            else:
                with source_connection(read_only=False) as connection:
                    results = inject(
                        scenario_names,
                        connection=connection,
                        partition_date=partition_date,
                        seed=seed,
                    )
                summary["faults"] = [result.as_dict() for result in results]

    if args.json:
        print(json.dumps(summary, indent=2, default=str))
    else:
        counts = summary["row_counts"]
        print(f"run_id      : {summary['run_id']}")
        print(f"seed        : {seed}")
        print(f"date        : {partition_date}")
        print(f"batch_id    : {batch_id}")
        print(f"fingerprint : {summary['fingerprint']}")
        print(f"rows        : {counts}")
        if summary.get("parquet_written"):
            for name, info in summary["parquet_written"].items():  # type: ignore[union-attr]
                print(f"parquet     : {name} -> {info.get('rows')} rows")
        for fault in summary.get("faults", []) or []:  # type: ignore[union-attr]
            print(
                f"fault       : {fault['scenario']} -> {fault['rows_affected']} rows "
                f"(expect {fault['expected_detector']}/{fault['expected_severity']})"
            )

    return 0


if __name__ == "__main__":
    sys.exit(main())
