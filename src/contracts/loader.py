"""Load and validate contract YAML into typed objects.

Two things matter here beyond plain parsing:

1. **Strictness.** An unknown top-level key, a misspelled severity or a
   dimension that is not one of the six is an error. A loader that shrugged at
   ``severiy: CRITICAL`` would produce a rule that silently runs at the
   default severity and never fails a gate.

2. **Checksums.** The sha256 is taken over the *normalised* content (parsed
   and re-serialised with sorted keys), not over the file bytes. Reformatting
   or reordering a contract therefore does not look like a change, while
   altering a threshold does - even if the author forgot to bump the version.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import yaml

from src.common.logging import get_logger
from src.common.severity import Severity
from src.contracts.models import (
    DIMENSIONS,
    CategoricalDriftSpec,
    ColumnSpec,
    Contract,
    ContractError,
    DistributionSpec,
    FreshnessSpec,
    NumericDriftSpec,
    QualityRule,
    SchemaSpec,
    ScoreSpec,
    SourceSpec,
    VolumeBaselineSpec,
    VolumeSpec,
)

LOGGER = get_logger(__name__)

_TOP_LEVEL_KEYS = {
    "dataset",
    "version",
    "owner",
    "criticality",
    "layer",
    "description",
    "source",
    "freshness",
    "volume",
    "schema",
    "distribution",
    "quality_rules",
    "score",
}


def _reject_unknown(section: str, payload: dict[str, Any], allowed: set[str]) -> None:
    unknown = set(payload) - allowed
    if unknown:
        raise ContractError(
            f"{section}: unknown key(s) {sorted(unknown)}; allowed are {sorted(allowed)}"
        )


def _require(section: str, payload: dict[str, Any], key: str) -> Any:
    if key not in payload:
        raise ContractError(f"{section}: required key {key!r} is missing")
    return payload[key]


def normalised_checksum(payload: dict[str, Any]) -> str:
    """sha256 over the parsed content with sorted keys."""
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Section parsers
# ---------------------------------------------------------------------------


def _parse_source(payload: dict[str, Any]) -> SourceSpec:
    _reject_unknown(
        "source",
        payload,
        {
            "type",
            "location",
            "event_timestamp_column",
            "ingestion_timestamp_column",
            "partition_column",
        },
    )
    source_type = _require("source", payload, "type")
    if source_type not in ("postgres", "parquet"):
        raise ContractError(f"source.type must be postgres or parquet, got {source_type!r}")
    return SourceSpec(
        type=source_type,
        location=_require("source", payload, "location"),
        event_timestamp_column=payload.get("event_timestamp_column"),
        ingestion_timestamp_column=payload.get("ingestion_timestamp_column"),
        partition_column=payload.get("partition_column"),
    )


def _parse_freshness(payload: dict[str, Any] | None) -> FreshnessSpec:
    if payload is None:
        return FreshnessSpec(enabled=False)
    _reject_unknown(
        "freshness",
        payload,
        {
            "enabled",
            "warning_after_minutes",
            "critical_after_minutes",
            "on_missing_timestamp",
        },
    )
    return FreshnessSpec(
        enabled=bool(payload.get("enabled", True)),
        warning_after_minutes=payload.get("warning_after_minutes"),
        critical_after_minutes=payload.get("critical_after_minutes"),
        on_missing_timestamp=Severity.parse(
            payload.get("on_missing_timestamp", "WARNING")
        ),
    )


def _parse_volume(payload: dict[str, Any] | None) -> VolumeSpec:
    if payload is None:
        return VolumeSpec(enabled=False)
    _reject_unknown(
        "volume", payload, {"enabled", "min_rows", "max_rows", "severity", "baseline"}
    )
    baseline_payload = payload.get("baseline") or {}
    _reject_unknown(
        "volume.baseline",
        baseline_payload,
        {
            "method",
            "window_days",
            "min_history_points",
            "warning_z",
            "critical_z",
            "same_day_type",
        },
    )
    baseline = VolumeBaselineSpec(
        method=baseline_payload.get("method", "rolling_median_mad"),
        window_days=int(baseline_payload.get("window_days", 28)),
        min_history_points=int(baseline_payload.get("min_history_points", 5)),
        warning_z=float(baseline_payload.get("warning_z", 3.5)),
        critical_z=float(baseline_payload.get("critical_z", 6.0)),
        same_day_type=bool(baseline_payload.get("same_day_type", True)),
    )
    if baseline.critical_z < baseline.warning_z:
        raise ContractError(
            "volume.baseline: critical_z must not be below warning_z "
            f"({baseline.critical_z} < {baseline.warning_z})"
        )
    return VolumeSpec(
        enabled=bool(payload.get("enabled", True)),
        min_rows=payload.get("min_rows"),
        max_rows=payload.get("max_rows"),
        severity=Severity.parse(payload.get("severity", "ERROR")),
        baseline=baseline,
    )


def _parse_schema(payload: dict[str, Any] | None) -> SchemaSpec:
    if payload is None:
        return SchemaSpec(enforce=False)
    _reject_unknown(
        "schema",
        payload,
        {
            "enforce",
            "columns",
            "check_nullability",
            "on_additive",
            "on_type_change",
            "on_column_removal",
            "on_rename_candidate",
            "on_breaking",
        },
    )
    columns: list[ColumnSpec] = []
    for entry in payload.get("columns") or []:
        _reject_unknown(
            "schema.columns[]", entry, {"name", "type", "nullable", "description"}
        )
        columns.append(
            ColumnSpec(
                name=_require("schema.columns[]", entry, "name"),
                type=_require("schema.columns[]", entry, "type"),
                nullable=bool(entry.get("nullable", True)),
                description=entry.get("description"),
            )
        )
    names = [column.name for column in columns]
    duplicates = {name for name in names if names.count(name) > 1}
    if duplicates:
        raise ContractError(f"schema.columns: duplicate column(s) {sorted(duplicates)}")
    return SchemaSpec(
        enforce=bool(payload.get("enforce", True)),
        columns=tuple(columns),
        check_nullability=bool(payload.get("check_nullability", False)),
        on_additive=Severity.parse(payload.get("on_additive", "INFO")),
        on_type_change=Severity.parse(payload.get("on_type_change", "CRITICAL")),
        on_column_removal=Severity.parse(payload.get("on_column_removal", "CRITICAL")),
        on_rename_candidate=Severity.parse(payload.get("on_rename_candidate", "ERROR")),
        on_breaking=Severity.parse(payload.get("on_breaking", "CRITICAL")),
    )


def _parse_distribution(payload: dict[str, Any] | None) -> DistributionSpec:
    if payload is None:
        return DistributionSpec(enabled=False)
    _reject_unknown(
        "distribution",
        payload,
        {"enabled", "min_sample_size", "severity", "numeric", "categorical"},
    )
    numeric: list[NumericDriftSpec] = []
    for entry in payload.get("numeric") or []:
        _reject_unknown(
            "distribution.numeric[]",
            entry,
            {"column", "method", "bins", "warning", "critical"},
        )
        spec = NumericDriftSpec(
            column=_require("distribution.numeric[]", entry, "column"),
            method=entry.get("method", "psi"),
            bins=int(entry.get("bins", 10)),
            warning=float(entry.get("warning", 0.10)),
            critical=float(entry.get("critical", 0.25)),
        )
        if spec.method != "psi":
            raise ContractError(
                f"distribution.numeric[{spec.column}]: only psi is implemented, "
                f"got {spec.method!r}"
            )
        if spec.critical < spec.warning:
            raise ContractError(
                f"distribution.numeric[{spec.column}]: critical must not be below warning"
            )
        numeric.append(spec)

    categorical: list[CategoricalDriftSpec] = []
    for entry in payload.get("categorical") or []:
        _reject_unknown(
            "distribution.categorical[]",
            entry,
            {"column", "method", "alpha", "min_expected_frequency", "max_categories"},
        )
        spec = CategoricalDriftSpec(
            column=_require("distribution.categorical[]", entry, "column"),
            method=entry.get("method", "chi_square"),
            alpha=float(entry.get("alpha", 0.01)),
            min_expected_frequency=int(entry.get("min_expected_frequency", 5)),
            max_categories=int(entry.get("max_categories", 20)),
        )
        if spec.method != "chi_square":
            raise ContractError(
                f"distribution.categorical[{spec.column}]: only chi_square is "
                f"implemented, got {spec.method!r}"
            )
        categorical.append(spec)

    return DistributionSpec(
        enabled=bool(payload.get("enabled", False)),
        min_sample_size=int(payload.get("min_sample_size", 500)),
        severity=Severity.parse(payload.get("severity", "WARNING")),
        numeric=tuple(numeric),
        categorical=tuple(categorical),
    )


def _parse_rules(payload: list[dict[str, Any]] | None) -> tuple[QualityRule, ...]:
    rules: list[QualityRule] = []
    seen: set[str] = set()
    for entry in payload or []:
        _reject_unknown(
            "quality_rules[]",
            entry,
            {
                "name",
                "dimension",
                "severity",
                "expectation",
                "column",
                "kwargs",
                "sql",
                "description",
                "max_unexpected_percent",
            },
        )
        name = _require("quality_rules[]", entry, "name")
        if name in seen:
            raise ContractError(f"quality_rules: duplicate rule name {name!r}")
        seen.add(name)

        dimension = _require("quality_rules[]", entry, "dimension")
        if dimension not in DIMENSIONS:
            raise ContractError(
                f"quality_rules[{name}]: dimension must be one of {list(DIMENSIONS)}, "
                f"got {dimension!r}"
            )

        rule = QualityRule(
            name=name,
            dimension=dimension,
            severity=Severity.parse(_require("quality_rules[]", entry, "severity")),
            expectation=_require("quality_rules[]", entry, "expectation"),
            column=entry.get("column"),
            kwargs=dict(entry.get("kwargs") or {}),
            sql=entry.get("sql"),
            description=entry.get("description"),
            max_unexpected_percent=float(entry.get("max_unexpected_percent", 0.0)),
        )
        # A SQL rule and a column rule are different shapes; asking for both
        # is almost certainly a mistake in the contract.
        if rule.sql is not None and rule.column is not None:
            raise ContractError(
                f"quality_rules[{name}]: a rule has either `sql` or `column`, not both"
            )
        rules.append(rule)
    return tuple(rules)


def _parse_score(payload: dict[str, Any] | None) -> ScoreSpec:
    if payload is None:
        return ScoreSpec()
    _reject_unknown("score", payload, {"weights", "healthy_at", "degraded_at"})
    weights = dict(payload.get("weights") or {})
    unknown = set(weights) - set(DIMENSIONS)
    if unknown:
        raise ContractError(f"score.weights: unknown dimension(s) {sorted(unknown)}")
    if weights:
        total = sum(float(value) for value in weights.values())
        # Allow a little float slack but reject a set that clearly does not
        # add up, which would make scores incomparable between datasets.
        if abs(total - 1.0) > 0.001:
            raise ContractError(
                f"score.weights must sum to 1.0, got {total:.4f}"
            )
    spec = ScoreSpec(
        weights={dim: float(value) for dim, value in weights.items()} or ScoreSpec().weights,
        healthy_at=float(payload.get("healthy_at", 90.0)),
        degraded_at=float(payload.get("degraded_at", 70.0)),
    )
    if spec.degraded_at > spec.healthy_at:
        raise ContractError("score.degraded_at must not exceed score.healthy_at")
    return spec


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def parse_contract(payload: dict[str, Any], *, source_path: str = "") -> Contract:
    """Validate a parsed YAML mapping into a Contract."""
    if not isinstance(payload, dict):
        raise ContractError(f"{source_path or 'contract'}: top level must be a mapping")
    _reject_unknown(source_path or "contract", payload, _TOP_LEVEL_KEYS)

    dataset = _require(source_path or "contract", payload, "dataset")
    version = str(_require(source_path or "contract", payload, "version"))
    criticality = payload.get("criticality", "medium")
    if criticality not in ("low", "medium", "high", "critical"):
        raise ContractError(
            f"{dataset}: criticality must be low/medium/high/critical, got {criticality!r}"
        )
    layer = payload.get("layer", "raw")
    if layer not in ("raw", "staging", "analytics", "lake"):
        raise ContractError(
            f"{dataset}: layer must be raw/staging/analytics/lake, got {layer!r}"
        )

    contract = Contract(
        dataset=dataset,
        version=version,
        owner=payload.get("owner"),
        criticality=criticality,
        layer=layer,
        description=payload.get("description"),
        source=_parse_source(_require(source_path or "contract", payload, "source")),
        freshness=_parse_freshness(payload.get("freshness")),
        volume=_parse_volume(payload.get("volume")),
        schema=_parse_schema(payload.get("schema")),
        distribution=_parse_distribution(payload.get("distribution")),
        quality_rules=_parse_rules(payload.get("quality_rules")),
        score=_parse_score(payload.get("score")),
        content_sha256=normalised_checksum(payload),
        source_path=source_path,
        raw=payload,
    )

    # Cross-section validation: a freshness check needs a timestamp to read.
    if contract.freshness.enabled and not (
        contract.source.event_timestamp_column
        or contract.source.ingestion_timestamp_column
    ):
        raise ContractError(
            f"{dataset}: freshness is enabled but neither "
            "source.event_timestamp_column nor source.ingestion_timestamp_column is set"
        )

    # A drift column must be a column the schema knows about, when the schema
    # is declared at all - otherwise the drift check would quietly never run.
    if contract.schema.columns:
        known = set(contract.schema.required_columns)
        for spec in contract.distribution.numeric:
            if spec.column not in known:
                raise ContractError(
                    f"{dataset}: distribution.numeric column {spec.column!r} is not in "
                    "the declared schema"
                )
        for spec in contract.distribution.categorical:
            if spec.column not in known:
                raise ContractError(
                    f"{dataset}: distribution.categorical column {spec.column!r} is not "
                    "in the declared schema"
                )

    return contract


def load_contract(path: str | Path) -> Contract:
    """Load one contract file."""
    file_path = Path(path)
    try:
        payload = yaml.safe_load(file_path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise ContractError(f"{file_path}: invalid YAML: {exc}") from exc
    return parse_contract(payload or {}, source_path=str(file_path))


def load_contracts(directory: str | Path) -> list[Contract]:
    """Load every contract in a directory, sorted by dataset name.

    Discovery is purely by file presence: this is what makes the platform
    extensible without code changes.
    """
    base = Path(directory)
    if not base.is_dir():
        raise ContractError(f"contracts directory not found: {base}")

    contracts: list[Contract] = []
    for file_path in sorted(base.glob("*.y*ml")):
        contract = load_contract(file_path)
        contracts.append(contract)
        LOGGER.info(
            "loaded contract %s v%s (%d rules) from %s",
            contract.dataset,
            contract.version,
            len(contract.quality_rules),
            file_path.name,
        )

    names = [contract.dataset for contract in contracts]
    duplicates = {name for name in names if names.count(name) > 1}
    if duplicates:
        raise ContractError(
            f"two contract files declare the same dataset(s): {sorted(duplicates)}"
        )
    if not contracts:
        raise ContractError(f"no contract files found in {base}")

    return sorted(contracts, key=lambda contract: contract.dataset)
