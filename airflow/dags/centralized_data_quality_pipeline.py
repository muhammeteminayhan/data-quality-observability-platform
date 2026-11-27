"""DAG: centralized_data_quality_pipeline

Seventeen tasks, grouped into six TaskGroups, with the quality gate in the
middle. The gate FAILS the DAG run on a CRITICAL finding, which is the whole
design: a failed quality check must never surface as a successful pipeline run.

    ingest --> validate --> observe --> gate --> transform --> respond

Task list
---------
  1  open_run                     (ingest)    the correlating pipeline_runs row
  2  register_contracts           (ingest)    contracts/*.yml -> datasets
  3  generate_source_data         (ingest)    deterministic Faker -> PG + MinIO
  4  inject_faults                (ingest)    only when the DAG param asks
  5  validate_postgres_datasets   (validate)  GX against commerce_db
  6  validate_parquet_datasets    (validate)  GX against MinIO Parquet
  7  check_freshness              (observe)
  8  check_volume                 (observe)
  9  track_schema_drift           (observe)
 10  check_distribution_drift     (observe)
 11  compute_quality_scores       (observe)
 12  quality_gate                 (gate)      raises on CRITICAL
 13  dbt_build                    (transform) models + tests
 14  parse_dbt_results            (transform) artifacts -> observability_db
 15  reconcile_incidents          (respond)   open / dedupe / resolve
 16  dispatch_alerts              (respond)   webhook, delivery recorded
 17  close_run                    (respond)   final status on pipeline_runs

Parameters
----------
    fault_profile   "clean" (default) or a profile/scenario name such as
                    "volume_drop", so a manual run can inject a fault and
                    watch the right detector catch it.
    records         number of orders to generate
    seed            generator seed; the same seed reproduces the dataset
    reload_source   replace the source tables rather than append
    generate_data   off to observe whatever is already there

Lineage
-------
The OpenLineage Airflow provider emits START and COMPLETE/FAIL for every task
on its own. It cannot see inside a Python callable, so the tasks that read
Postgres and MinIO directly also emit their datasets explicitly through
src/lineage/emitter.py. Both kinds are real events from running tasks; nothing
is drawn from a static description of the pipeline.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

import pendulum
from airflow.sdk import Param, dag, task, task_group

DAG_ID = "centralized_data_quality_pipeline"

DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=5),
    # A task that hangs holds a worker slot and delays everything behind it.
    "execution_timeout": timedelta(minutes=20),
    "depends_on_past": False,
}

# Tasks the gate blocks when it fails, recorded on the decision so the reason
# a downstream task did not run is queryable rather than inferred.
DOWNSTREAM_OF_GATE = ["transform.dbt_build", "transform.parse_dbt_results"]

RAW_TABLES = ("customers", "products", "orders", "order_items", "payments")
LAKE_DATASETS = ("daily_sales", "inventory_snapshots", "customer_activity")
ANALYTICS_MODELS = (
    "dim_customers",
    "dim_products",
    "fct_orders",
    "fct_order_items",
    "agg_daily_sales",
    "agg_customer_lifetime",
)


def _partition_date(context: dict[str, Any]):
    """The partition date for this run, however Airflow supplied it.

    Airflow 3 made `logical_date` OPTIONAL: a manually triggered run has no
    data interval, so the key is absent or None and reading it directly raises
    `KeyError: logical_date`. The fallbacks are tried in order of how well
    they describe the partition being processed, and the final one is today -
    which is what a manual "run it now" means.
    """
    for key in ("logical_date", "data_interval_start", "start_date"):
        value = context.get(key)
        if value is None:
            continue
        return value.date() if hasattr(value, "date") else value
    return datetime.now(timezone.utc).date()


@dag(
    dag_id=DAG_ID,
    description=(
        "Centralized data quality and observability: generate, validate, "
        "observe, gate, transform, respond."
    ),
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["data-quality", "observability", "great-expectations", "dbt", "openlineage"],
    doc_md=__doc__,
    params={
        "fault_profile": Param(
            "clean",
            type="string",
            title="Fault profile",
            description=(
                "clean, a family name (missing_values, duplicates, "
                "invalid_values, referential_integrity, financial_inconsistency, "
                "volume_drop, volume_spike, freshness, schema_drift, "
                "distribution_drift, all_families) or a single scenario name."
            ),
        ),
        "records": Param(50000, type="integer", minimum=100, title="Orders to generate"),
        "seed": Param(20240115, type="integer", title="Generator seed"),
        "reload_source": Param(
            True,
            type="boolean",
            title="Replace the source tables",
            description=(
                "The generator emits the same natural keys for any date, so "
                "appending would create genuine duplicate-key failures."
            ),
        ),
        "generate_data": Param(
            True,
            type="boolean",
            title="Generate data",
            description="Turn off to observe whatever is already there.",
        ),
    },
)
def centralized_data_quality_pipeline() -> None:
    @task_group(group_id="ingest")
    def ingest() -> Any:
        @task
        def open_run(**context: Any) -> str:
            """Task 1: the row every other task correlates to."""
            from src.common.run_context import ensure_run, from_airflow_context

            run_context = from_airflow_context(context)
            ensure_run(run_context)
            print(
                f"run_id={run_context.run_id} date={run_context.logical_date} "
                f"fault_profile={run_context.fault_profile}"
            )
            return run_context.run_id_str

        @task
        def register_contracts() -> dict[str, Any]:
            """Task 2: contracts are the only source of monitored datasets."""
            from src.observability.tasks import register_contracts as register

            return register()

        @task
        def generate_source_data(platform_run_id: str, **context: Any) -> dict[str, Any]:
            """Task 3: deterministic synthetic data into Postgres and MinIO."""
            from src.generator import parquet_sink, postgres_sink
            from src.generator.cli import scaled_volumes
            from src.generator.records import generate
            from src.lineage import emitter

            params = context["params"]
            if not params["generate_data"]:
                print("generate_data is off; leaving the source as it is")
                return {"skipped": True}

            partition_date = _partition_date(context)
            seed = int(params["seed"])
            batch_id = f"{partition_date.isoformat()}-{seed}"
            volumes = scaled_volumes(int(params["records"]), partition_date=partition_date)
            replace_source = bool(params["reload_source"])

            outputs = [emitter.source_dataset(f"raw.{n}") for n in RAW_TABLES] + [
                emitter.lake_dataset(n, partition_date.isoformat()) for n in LAKE_DATASETS
            ]

            # Explicit lineage: the provider cannot extract datasets from a
            # Python callable that uses psycopg and boto3 directly.
            with emitter.tracked(
                job_name=f"{DAG_ID}.generate_source_data",
                run_id=platform_run_id,
                inputs=[],
                outputs=outputs,
                description="Deterministic Faker data into commerce_db and the lake",
            ):
                data = generate(
                    seed=seed,
                    partition_date=partition_date,
                    batch_id=batch_id,
                    volumes=volumes,
                )
                written = postgres_sink.write(data, truncate=replace_source)
                lake = parquet_sink.write(data)

            print(f"seed={seed} fingerprint={data.fingerprint()}")
            return {
                "batch_id": batch_id,
                "postgres": written,
                "parquet": {k: v.get("rows") for k, v in lake.items()},
                "fingerprint": data.fingerprint(),
            }

        @task
        def inject_faults(platform_run_id: str, **context: Any) -> dict[str, Any]:
            """Task 4: reproducible fault injection, driven by the DAG param."""
            from src.common.db import source_connection
            from src.generator.fault_injector import inject, resolve_profile

            params = context["params"]
            profile = str(params["fault_profile"])
            scenarios = list(resolve_profile(profile))
            if not scenarios:
                print(f"fault_profile={profile!r} injects nothing (clean run)")
                return {"profile": profile, "scenarios": [], "rows_affected": 0}

            with source_connection(read_only=False) as connection:
                results = inject(
                    scenarios,
                    connection=connection,
                    partition_date=_partition_date(context),
                    seed=int(params["seed"]),
                )
            for result in results:
                print(
                    f"{result.scenario}: {result.rows_affected} rows -> expect "
                    f"{result.expected_detector}/{result.expected_severity}"
                )
            return {
                "profile": profile,
                "scenarios": [r.as_dict() for r in results],
                "rows_affected": sum(r.rows_affected for r in results),
            }

        run_id = open_run()
        contracts = register_contracts()
        generated = generate_source_data(run_id)
        faults = inject_faults(run_id)
        run_id >> contracts >> generated >> faults
        return run_id, faults

    @task_group(group_id="validate")
    def validate(platform_run_id: str, upstream: dict[str, Any]) -> Any:
        @task
        def validate_postgres_datasets(
            platform_run_id: str, upstream: dict[str, Any], **context: Any
        ) -> dict[str, Any]:
            """Task 5: GX Core against commerce_db."""
            from src.lineage import emitter
            from src.observability.tasks import validate_datasets

            with emitter.tracked(
                job_name=f"{DAG_ID}.validate_postgres_datasets",
                run_id=platform_run_id,
                inputs=[emitter.source_dataset(f"raw.{n}") for n in RAW_TABLES],
                outputs=[
                    emitter.observability_dataset("validation_runs"),
                    emitter.observability_dataset("validation_results"),
                ],
                description="Great Expectations suites built from the contracts",
            ):
                return validate_datasets(
                    run_id=uuid.UUID(platform_run_id),
                    partition_date=_partition_date(context),
                    source_type="postgres",
                )

        @task
        def validate_parquet_datasets(
            platform_run_id: str, upstream: dict[str, Any], **context: Any
        ) -> dict[str, Any]:
            """Task 6: GX Core against the Parquet datasets in MinIO."""
            from src.lineage import emitter
            from src.observability.tasks import validate_datasets

            partition_date = _partition_date(context)
            with emitter.tracked(
                job_name=f"{DAG_ID}.validate_parquet_datasets",
                run_id=platform_run_id,
                inputs=[
                    emitter.lake_dataset(n, partition_date.isoformat())
                    for n in LAKE_DATASETS
                ],
                outputs=[emitter.observability_dataset("validation_results")],
                description="GX against the lake via a pandas_s3 file datasource",
            ):
                return validate_datasets(
                    run_id=uuid.UUID(platform_run_id),
                    partition_date=partition_date,
                    source_type="parquet",
                )

        return [
            validate_postgres_datasets(platform_run_id, upstream),
            validate_parquet_datasets(platform_run_id, upstream),
        ]

    @task_group(group_id="observe")
    def observe(platform_run_id: str, upstream: Any) -> Any:
        @task
        def check_freshness(
            platform_run_id: str, upstream: Any, **context: Any
        ) -> dict[str, Any]:
            """Task 7: freshness from the dataset's own clock."""
            from src.observability.tasks import check_freshness as run_check

            return run_check(
                run_id=uuid.UUID(platform_run_id), partition_date=_partition_date(context)
            )

        @task
        def check_volume(
            platform_run_id: str, upstream: Any, **context: Any
        ) -> dict[str, Any]:
            """Task 8: robust median/MAD baseline, same-day-type."""
            from src.observability.tasks import check_volume as run_check

            return run_check(
                run_id=uuid.UUID(platform_run_id), partition_date=_partition_date(context)
            )

        @task
        def track_schema_drift(
            platform_run_id: str, upstream: Any, **context: Any
        ) -> dict[str, Any]:
            """Task 9: fingerprint and classify."""
            from src.observability.tasks import track_schema_drift as run_check

            return run_check(
                run_id=uuid.UUID(platform_run_id), partition_date=_partition_date(context)
            )

        @task
        def check_distribution_drift(
            platform_run_id: str, upstream: Any, **context: Any
        ) -> dict[str, Any]:
            """Task 10: PSI and chi-square, with inconclusive as an outcome."""
            from src.observability.tasks import check_distribution_drift as run_check

            return run_check(
                run_id=uuid.UUID(platform_run_id), partition_date=_partition_date(context)
            )

        @task
        def compute_quality_scores(platform_run_id: str) -> dict[str, Any]:
            """Task 11: the weighted score over the measured dimensions."""
            from src.observability.tasks import compute_quality_scores as run_scores

            return run_scores(run_id=uuid.UUID(platform_run_id))

        fresh = check_freshness(platform_run_id, upstream)
        vol = check_volume(platform_run_id, upstream)
        schema = track_schema_drift(platform_run_id, upstream)
        drift = check_distribution_drift(platform_run_id, upstream)
        scores = compute_quality_scores(platform_run_id)
        [fresh, vol, schema, drift] >> scores
        return scores

    @task
    def quality_gate(platform_run_id: str, upstream: Any) -> dict[str, Any]:
        """Task 12: stop the pipeline on a CRITICAL finding.

        This task RAISES on FAIL. The decision is recorded first, then the
        exception marks the task - and so the DAG run - as failed. A gate that
        only logged would let a broken pipeline look green, which the
        specification forbids.
        """
        from airflow.exceptions import AirflowException

        from src.gates.gate import QualityGateFailed
        from src.observability.tasks import run_quality_gate

        try:
            return run_quality_gate(
                run_id=uuid.UUID(platform_run_id), downstream_tasks=DOWNSTREAM_OF_GATE
            )
        except QualityGateFailed as failure:
            decision = failure.decision
            for signal in decision.signals:
                print(
                    f"{signal.dataset}: {signal.max_severity.value} "
                    f"{', '.join(signal.failed_rules) or signal.detail}"
                )
            raise AirflowException(
                f"quality gate {decision.decision}: {decision.reason}"
            ) from failure

    @task_group(group_id="transform")
    def transform(platform_run_id: str, gate: dict[str, Any]) -> Any:
        @task
        def dbt_build(platform_run_id: str, gate: dict[str, Any]) -> dict[str, Any]:
            """Task 13: dbt models and tests.

            dbt runs from its isolated venv (DBT_BIN) because dbt requires
            pathspec<1.1 while Airflow pins 1.1.1 and uses it for
            .airflowignore.
            """
            from src.dbt_runner.run import invoke
            from src.lineage import emitter

            staging = [emitter.source_dataset(f"staging.stg_{n}") for n in RAW_TABLES]
            analytics = [
                emitter.source_dataset(f"analytics.{n}") for n in ANALYTICS_MODELS
            ]
            with emitter.tracked(
                job_name=f"{DAG_ID}.dbt_build",
                run_id=platform_run_id,
                inputs=[emitter.source_dataset(f"raw.{n}") for n in RAW_TABLES],
                outputs=staging + analytics,
                description="dbt build: raw -> staging -> analytics, with tests",
            ):
                invocation = invoke(command="build")

            if invocation.summary is None:
                raise RuntimeError(
                    "dbt produced no artifacts, so nothing can be said about the "
                    f"data. exit={invocation.returncode}"
                )
            summary = invocation.summary
            return {
                "returncode": invocation.returncode,
                "invocation_id": summary.invocation_id,
                "models_total": len(summary.models),
                "models_success": summary.models_success,
                "tests_total": len(summary.tests),
                "tests_pass": summary.tests_pass,
                "tests_fail": summary.tests_fail,
            }

        @task
        def parse_dbt_results(platform_run_id: str, build: dict[str, Any]) -> dict[str, Any]:
            """Task 14: artifacts -> observability_db.

            A separate task from the build so a parsing bug cannot be mistaken
            for a dbt failure, and so results are recorded even when the build
            itself failed.
            """
            from src.common.config import get_settings
            from src.dbt_runner.artifacts import parse
            from src.dbt_runner.run import persist

            project = get_settings().dbt_project_dir
            summary = parse(
                project / "target" / "run_results.json",
                manifest_path=project / "target" / "manifest.json",
                command="dbt build",
            )
            dbt_run_id = persist(summary, run_id=uuid.UUID(platform_run_id))
            print(
                f"models {summary.models_success}/{len(summary.models)} executed; "
                f"tests {summary.tests_pass}/{len(summary.tests)} passed. A model "
                "succeeding means the SQL ran, not that the output is right."
            )
            return {
                "dbt_run_id": dbt_run_id,
                "data_is_trustworthy": summary.data_is_trustworthy,
                "max_severity": summary.max_severity.value,
            }

        built = dbt_build(platform_run_id, gate)
        return parse_dbt_results(platform_run_id, built)

    @task_group(group_id="respond")
    def respond(platform_run_id: str, upstream: Any) -> Any:
        @task(trigger_rule="all_done")
        def reconcile_incidents(
            platform_run_id: str, upstream: Any
        ) -> dict[str, Any]:
            """Task 15: open, deduplicate, and resolve only on real evidence."""
            from src.observability.tasks import reconcile_incidents as reconcile

            return reconcile(run_id=uuid.UUID(platform_run_id))

        @task
        def dispatch_alerts(platform_run_id: str, reconciliation: dict[str, Any]) -> dict[str, Any]:
            """Task 16: deliver alerts and record every attempt."""
            from src.observability.tasks import dispatch_alerts as dispatch

            return dispatch(
                run_id=uuid.UUID(platform_run_id), alertable=reconciliation.get("alertable")
            )

        @task(trigger_rule="all_done")
        def close_run(platform_run_id: str) -> dict[str, Any]:
            """Task 17: final status.

            `all_done` so the run is always closed, including when the gate
            failed. `blocked` (the gate stopped it on purpose) is recorded
            distinctly from `failed` (something broke) - they need different
            responses.
            """
            from airflow.exceptions import AirflowException

            from src.common.db import fetch_one, observability_connection
            from src.common.run_context import finish_run

            identifier = uuid.UUID(platform_run_id)
            with observability_connection() as connection:
                gate = fetch_one(
                    connection,
                    """
                    SELECT decision, reason FROM gate_decisions
                    WHERE run_id = %s ORDER BY decided_at DESC LIMIT 1
                    """,
                    (identifier,),
                )

            if gate is None:
                final = "failed"
                note = "no gate decision recorded; the run did not reach the gate"
            elif gate["decision"] == "FAIL":
                final = "blocked"
                note = gate["reason"]
            else:
                final = "success"
                note = gate["reason"]

            finish_run(identifier, final, notes=note)
            print(f"run {platform_run_id} closed as {final}: {note}")

            # The DAG run's state comes from its LEAF tasks. close_run uses
            # trigger_rule="all_done" so the run is always closed, which also
            # makes it the leaf that decides the colour of the whole run.
            #
            # Observed for real: with close_run simply returning, a run in
            # which five tasks failed and the gate never executed was recorded
            # by Airflow as SUCCESS. That is exactly the outcome the
            # specification forbids - a failed quality check must not surface
            # as a successful pipeline run - so the task records the outcome
            # first and then fails, leaving the run red and the reason stored.
            if final != "success":
                raise AirflowException(
                    f"pipeline run closed as {final}: {note}"
                )
            return {"status": final, "note": note}

        reconciled = reconcile_incidents(platform_run_id, upstream)
        alerts = dispatch_alerts(platform_run_id, reconciled)
        closed = close_run(platform_run_id)
        alerts >> closed
        return closed

    # Every stage receives the previous stage's result as an ARGUMENT, so the
    # dependency is a data dependency Airflow cannot bypass. The earlier
    # version chained the groups with `>>` on the XComArg each group returned,
    # which constrained only that one leaf task - dbt_build still depended on
    # open_run alone and ran even though the gate had not passed.
    run_id, faults = ingest()
    validated = validate(run_id, faults)
    observed = observe(run_id, validated)
    gate = quality_gate(run_id, observed)
    transformed = transform(run_id, gate)

    # respond hangs off OBSERVE, not off transform. A gate FAIL must still
    # open the incident and send the alert - that is the whole point of the
    # failure. Only the transform work is gated.
    responded = respond(run_id, observed)
    transformed >> responded


centralized_data_quality_pipeline()
