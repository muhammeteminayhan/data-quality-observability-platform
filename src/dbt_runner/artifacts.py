"""Parse dbt artifacts into the platform's own result types.

`target/run_results.json` is dbt's record of what it executed. It is parsed
here rather than scraped from stdout, because the JSON carries the per-node
status, timing, row counts and failure counts that a log line does not.

The distinction this module exists to preserve
----------------------------------------------
dbt reports MODEL results and TEST results in the same file, with the same
`status` vocabulary. They mean completely different things:

* a model with status `success` means **the SQL ran**. It says nothing about
  whether the output is correct. A model can happily materialise a table full
  of wrong numbers and report success.
* a test with status `pass` means **an assertion about the data held**.

Conflating them is how a pipeline comes to be described as healthy because
`dbt run` exited 0. So the counts are stored in separate columns
(`models_success` vs `tests_pass`), the per-node rows carry `resource_type`,
and `dbt_runs.success` is deliberately NOT the same thing as the data being
right. The quality score and the gate read the TEST results.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from src.common.logging import get_logger
from src.common.severity import Severity

LOGGER = get_logger(__name__)

# dbt statuses that mean the node did its job.
MODEL_OK = {"success"}
TEST_OK = {"pass"}
# A dbt test configured with severity=warn reports `warn`, which is a real
# finding but not a failure.
TEST_WARN = {"warn"}


@dataclass
class DbtNodeResult:
    unique_id: str
    node_name: str
    resource_type: str
    status: str
    execution_time: float | None = None
    rows_affected: int | None = None
    failures: int | None = None
    message: str | None = None
    test_type: str | None = None
    severity: Severity | None = None
    depends_on: list[str] = field(default_factory=list)
    relation: str | None = None
    # From the model's `config.meta.dq_dataset`. The authoritative link
    # between a dbt node and a monitored dataset.
    dq_dataset: str | None = None

    @property
    def is_test(self) -> bool:
        return self.resource_type == "test"

    @property
    def is_model(self) -> bool:
        return self.resource_type == "model"


@dataclass
class DbtRunSummary:
    invocation_id: str
    command: str
    dbt_version: str | None = None
    dbt_schema_version: str | None = None
    started_at: str | None = None
    finished_at: str | None = None
    elapsed_seconds: float | None = None
    success: bool = False
    nodes: list[DbtNodeResult] = field(default_factory=list)

    # -- model counts (SQL executed) -------------------------------------
    @property
    def models(self) -> list[DbtNodeResult]:
        return [n for n in self.nodes if n.is_model]

    @property
    def models_success(self) -> int:
        return sum(1 for n in self.models if n.status in MODEL_OK)

    @property
    def models_error(self) -> int:
        return sum(1 for n in self.models if n.status == "error")

    @property
    def models_skipped(self) -> int:
        return sum(1 for n in self.models if n.status == "skipped")

    # -- test counts (assertions about the data) -------------------------
    @property
    def tests(self) -> list[DbtNodeResult]:
        return [n for n in self.nodes if n.is_test]

    @property
    def tests_pass(self) -> int:
        return sum(1 for n in self.tests if n.status in TEST_OK)

    @property
    def tests_fail(self) -> int:
        return sum(1 for n in self.tests if n.status in {"fail", "error"})

    @property
    def tests_warn(self) -> int:
        return sum(1 for n in self.tests if n.status in TEST_WARN)

    @property
    def tests_skipped(self) -> int:
        return sum(1 for n in self.tests if n.status == "skipped")

    @property
    def max_severity(self) -> Severity:
        """Severity of the DATA findings, not of the execution.

        A failed test is an ERROR about the data. A failed MODEL is also
        serious, but it is a pipeline failure rather than a quality finding,
        and it is reported through the task failing.
        """
        if self.tests_fail:
            return Severity.ERROR
        if self.tests_warn:
            return Severity.WARNING
        return Severity.INFO

    @property
    def data_is_trustworthy(self) -> bool:
        """Whether the ASSERTIONS held - not whether dbt exited 0."""
        return self.tests_fail == 0 and self.models_error == 0


def _load_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise FileNotFoundError(
            f"dbt artifact not found at {path}. dbt writes it only after a run, "
            "so an absent file means the invocation never completed - which is "
            "itself a failure, not an empty result set."
        ) from exc


def _dq_dataset(
    unique_id: str,
    manifest_node: dict[str, Any] | None,
    manifest_nodes: dict[str, Any],
) -> str | None:
    """The monitored dataset a node belongs to, from explicit metadata only.

    A model declares it as `config.meta.dq_dataset`. A TEST inherits it from
    the first model it depends on, because a test has no meta of its own.

    Never inferred from a name. dbt calls the model `stg_orders` while the
    contract calls the dataset `raw.orders`, and the obvious-looking string
    match would attach, say, a `daily_sales` finding to `agg_daily_sales` -
    two different objects in two different stores.
    """
    if manifest_node is None:
        return None

    own = ((manifest_node.get("config") or {}).get("meta") or {}).get("dq_dataset")
    if own:
        return str(own)

    for dependency in (manifest_node.get("depends_on") or {}).get("nodes") or []:
        if not dependency.startswith("model."):
            continue
        parent = manifest_nodes.get(dependency)
        if parent is None:
            continue
        inherited = ((parent.get("config") or {}).get("meta") or {}).get("dq_dataset")
        if inherited:
            return str(inherited)
    return None


def _test_type(unique_id: str, manifest_node: dict[str, Any] | None) -> str | None:
    """Tell a generic test from a singular one.

    dbt's manifest marks a generic test with `test_metadata`; a singular test
    is a plain SQL file and has none. Worth recording, because the two are
    maintained differently: a generic test comes from a YAML property and a
    singular one is hand-written SQL.
    """
    if manifest_node is None:
        return None
    if manifest_node.get("test_metadata"):
        return "generic"
    return "singular"


def _severity_of(manifest_node: dict[str, Any] | None, status: str) -> Severity | None:
    """Map a dbt test's configured severity onto the platform's scale."""
    if manifest_node is None:
        return None
    configured = str(
        (manifest_node.get("config") or {}).get("severity", "error")
    ).lower()
    if status in TEST_OK:
        return Severity.INFO
    if configured == "warn" or status in TEST_WARN:
        return Severity.WARNING
    return Severity.ERROR


def parse(
    run_results_path: str | Path,
    *,
    manifest_path: str | Path | None = None,
    command: str = "dbt build",
) -> DbtRunSummary:
    """Parse run_results.json, enriched from manifest.json when available."""
    results_file = Path(run_results_path)
    payload = _load_json(results_file)
    metadata = payload.get("metadata") or {}

    manifest_nodes: dict[str, Any] = {}
    if manifest_path is not None:
        manifest_file = Path(manifest_path)
        if manifest_file.exists():
            manifest = _load_json(manifest_file)
            manifest_nodes = manifest.get("nodes") or {}
        else:
            LOGGER.warning(
                "manifest not found at %s; test type and severity will be unknown",
                manifest_file,
            )

    summary = DbtRunSummary(
        invocation_id=metadata.get("invocation_id") or "unknown",
        command=command,
        dbt_version=metadata.get("dbt_version"),
        dbt_schema_version=metadata.get("dbt_schema_version"),
        started_at=payload.get("args", {}).get("invocation_started_at")
        or metadata.get("generated_at"),
        finished_at=metadata.get("generated_at"),
        elapsed_seconds=payload.get("elapsed_time"),
    )

    for result in payload.get("results") or []:
        unique_id = result.get("unique_id") or ""
        manifest_node = manifest_nodes.get(unique_id)
        resource_type = unique_id.split(".")[0] if unique_id else "unknown"
        if manifest_node:
            resource_type = manifest_node.get("resource_type", resource_type)

        status = str(result.get("status", "unknown"))
        summary.nodes.append(
            DbtNodeResult(
                unique_id=unique_id,
                node_name=(manifest_node or {}).get("name")
                or unique_id.split(".")[-1],
                resource_type=resource_type,
                status=status,
                execution_time=result.get("execution_time"),
                rows_affected=(result.get("adapter_response") or {}).get("rows_affected"),
                # dbt reports a test's offending row count as `failures`.
                failures=result.get("failures"),
                message=result.get("message"),
                test_type=_test_type(unique_id, manifest_node)
                if resource_type == "test"
                else None,
                severity=_severity_of(manifest_node, status)
                if resource_type == "test"
                else None,
                depends_on=list(
                    ((manifest_node or {}).get("depends_on") or {}).get("nodes") or []
                ),
                relation=(manifest_node or {}).get("relation_name"),
                dq_dataset=_dq_dataset(unique_id, manifest_node, manifest_nodes),
            )
        )

    # `success` here means every node dbt ran reported an acceptable status.
    # It is NOT a statement that the data is correct - see the module docstring.
    summary.success = all(
        node.status in (MODEL_OK | TEST_OK | TEST_WARN | {"skipped"})
        for node in summary.nodes
    )

    LOGGER.info(
        "parsed dbt invocation %s: %d models (%d ok, %d error), "
        "%d tests (%d pass, %d fail, %d warn) in %.2fs",
        summary.invocation_id,
        len(summary.models),
        summary.models_success,
        summary.models_error,
        len(summary.tests),
        summary.tests_pass,
        summary.tests_fail,
        summary.tests_warn,
        summary.elapsed_seconds or 0.0,
    )
    return summary


def dataset_for_node(node: DbtNodeResult) -> str | None:
    """Best-effort mapping from a dbt node to a monitored dataset name.

    A test depends on the models it asserts about, so the first model
    dependency names the dataset the finding belongs to. Returns None when the
    node cannot be attributed - which is recorded as NULL rather than guessed,
    so a dashboard never blames the wrong table.
    """
    if node.dq_dataset:
        return node.dq_dataset
    # No explicit mapping: recorded as NULL rather than guessed, so a
    # dashboard never blames the wrong table. Models without a
    # `dq_dataset` meta (the analytics layer, in this version) are counted in
    # dbt_runs but not attributed to a dataset - a documented limitation.
    return None
