"""Schema tracking: fingerprint every run, classify what changed.

Fingerprint
-----------
sha256 over the ORDERED (name, type, nullable) triples. Ordered, because a
column reordering is itself a change worth seeing - a downstream consumer
reading by position would silently start reading the wrong field.

Classification
--------------
    ADDITIVE                 a new column appeared; nothing existing moved
    TYPE_CHANGE              a column kept its name and changed type
    COLUMN_REMOVAL           a column the contract declares has gone
    COLUMN_RENAME_CANDIDATE  one column vanished and another appeared with a
                             compatible type in the same run
    BREAKING                 a required column became nullable, or a declared
                             column is missing from the observed schema

Why rename is only ever a CANDIDATE
-----------------------------------
A rename and a drop-plus-add produce *identical* observations. Nothing in the
schema, and nothing in the data, can separate them: the values under the new
name may be the old column's values, or they may be a brand new field that
happens to share a type. Calling it a rename would be asserting intent the
platform cannot observe. So the change is recorded as a candidate with a
confidence score, `is_confirmed` stays false, and only a human or an explicit
contract change can promote it.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date
from typing import Any

from src.common.db import describe_columns, source_connection
from src.common.logging import get_logger
from src.common.severity import Severity
from src.common.storage import read_partition
from src.contracts.models import Contract

LOGGER = get_logger(__name__)

# Types that are interchangeable enough that a change between them is not
# worth reporting as a TYPE_CHANGE.
_TYPE_ALIASES: dict[str, str] = {
    "timestamp with time zone": "timestamptz",
    "timestamp without time zone": "timestamp",
    "character varying": "text",
    "varchar": "text",
    "string": "text",
    "str": "text",
    "large_string": "text",
    "int8": "bigint",
    "int64": "bigint",
    "int4": "integer",
    "int32": "integer",
    "float8": "double",
    "float64": "double",
    "double precision": "double",
    "numeric": "numeric",
    "decimal": "numeric",
    "bool": "boolean",
    "datetime64[ns]": "timestamp",
    "datetime64[us]": "timestamp",
    "datetime64[ns, utc]": "timestamptz",
    "datetime64[us, utc]": "timestamptz",
    "date32[day]": "date",
    "date32": "date",
}


def normalise_type(raw: str) -> str:
    key = str(raw).strip().lower()
    return _TYPE_ALIASES.get(key, key)


def _parquet_column_type(frame, name, dtype) -> str:
    """Best available type name for a Parquet/pandas column.

    pandas 3.0 reports a dedicated `str` dtype for strings, but a column of
    Python `date` objects still arrives as the opaque `object` dtype. Reporting
    "object" would make every date column look like a type change against a
    contract that correctly declares `date`, so an `object` column is resolved
    by inspecting its first non-null value. That is a real observation rather
    than an assumption, and it is the only way to tell a date column from a
    free-form one at this level.
    """
    label = str(dtype)
    if label != "object":
        return label
    series = frame[name].dropna()
    if series.empty:
        return "object"
    sample = series.iloc[0]
    import datetime as _dt

    if isinstance(sample, _dt.datetime):
        return "timestamptz" if sample.tzinfo else "timestamp"
    if isinstance(sample, _dt.date):
        return "date"
    if isinstance(sample, bool):
        return "boolean"
    if isinstance(sample, str):
        return "text"
    return type(sample).__name__


@dataclass(frozen=True)
class ObservedColumn:
    name: str
    type: str
    nullable: bool
    position: int

    def as_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "type": self.type,
            "nullable": self.nullable,
            "position": self.position,
        }


@dataclass
class ObservedSchema:
    dataset: str
    columns: tuple[ObservedColumn, ...]

    @property
    def fingerprint(self) -> str:
        payload = [
            [column.name, normalise_type(column.type), column.nullable]
            for column in self.columns
        ]
        canonical = json.dumps(payload, separators=(",", ":"))
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    @property
    def names(self) -> tuple[str, ...]:
        return tuple(column.name for column in self.columns)

    def column(self, name: str) -> ObservedColumn | None:
        for column in self.columns:
            if column.name == name:
                return column
        return None

    def as_json(self) -> list[dict[str, Any]]:
        return [column.as_dict() for column in self.columns]


@dataclass
class SchemaChange:
    change_type: str
    severity: Severity
    column_name: str | None = None
    previous_column: str | None = None
    previous_type: str | None = None
    new_type: str | None = None
    confidence: float | None = None
    is_confirmed: bool = False
    detail: str = ""


def snapshot(contract: Contract, partition_date: date) -> ObservedSchema:
    """Observe the current schema of a dataset."""
    if contract.source.type == "postgres":
        with source_connection() as connection:
            rows = describe_columns(connection, contract.source.location)
        columns = tuple(
            ObservedColumn(
                name=row["column_name"],
                type=row["data_type"],
                nullable=bool(row["is_nullable"]),
                position=int(row["ordinal_position"]),
            )
            for row in rows
        )
    else:
        frame = read_partition(contract.source.location, partition_date.isoformat())
        columns = tuple(
            ObservedColumn(
                name=str(name),
                type=_parquet_column_type(frame, name, dtype),
                # Parquet/pandas has no per-column NOT NULL. Reporting
                # "nullable" for everything would be a false statement, so
                # nullability is inferred from the data actually present and
                # the detail says so.
                nullable=bool(frame[name].isna().any()) if not frame.empty else True,
                position=index + 1,
            )
            for index, (name, dtype) in enumerate(frame.dtypes.items())
        )
    return ObservedSchema(dataset=contract.dataset, columns=columns)


def _rename_candidates(
    removed: list[str],
    added: list[str],
    previous: ObservedSchema,
    current: ObservedSchema,
) -> list[tuple[str, str, float]]:
    """Pair removals with additions of a compatible type.

    Confidence is deliberately modest and never 1.0:
      0.60  the types match
      +0.15 exactly one column went and exactly one arrived
      +0.10 the two names share a prefix or suffix of 3+ characters

    Even at the top of that range the pairing remains a hypothesis.
    """
    pairs: list[tuple[str, str, float]] = []
    for gone in removed:
        old_column = previous.column(gone)
        if old_column is None:
            continue
        for arrived in added:
            new_column = current.column(arrived)
            if new_column is None:
                continue
            if normalise_type(old_column.type) != normalise_type(new_column.type):
                continue

            confidence = 0.60
            if len(removed) == 1 and len(added) == 1:
                confidence += 0.15
            if (
                gone[:3].lower() == arrived[:3].lower()
                or gone[-3:].lower() == arrived[-3:].lower()
            ):
                confidence += 0.10
            pairs.append((gone, arrived, round(min(confidence, 0.85), 3)))

    # With 2 removals and 2 additions the loop above yields 4 pairings for
    # what may be a single rename, which would inflate the finding count and
    # open four incidents for one change. Assign greedily by descending
    # confidence so each removed and each added column is claimed at most
    # once. The surviving pairing is still only a CANDIDATE - this reduces
    # noise, it does not add certainty.
    pairs.sort(key=lambda item: (-item[2], item[0], item[1]))
    chosen: list[tuple[str, str, float]] = []
    used_old: set[str] = set()
    used_new: set[str] = set()
    for gone, arrived, confidence in pairs:
        if gone in used_old or arrived in used_new:
            continue
        chosen.append((gone, arrived, confidence))
        used_old.add(gone)
        used_new.add(arrived)
    return chosen


def _contract_changes(current: ObservedSchema, contract: Contract) -> list[SchemaChange]:
    """Differences between the observed schema and the declared contract.

    Runs even on the first ever snapshot: a dataset that never matched its
    contract is broken now, not only once it changes.
    """
    schema_spec = contract.schema
    changes: list[SchemaChange] = []
    if not schema_spec.enforce:
        return changes

    for declared in schema_spec.columns:
        observed = current.column(declared.name)
        if observed is None:
            changes.append(
                SchemaChange(
                    change_type="COLUMN_REMOVAL",
                    severity=schema_spec.severity_for("COLUMN_REMOVAL"),
                    column_name=declared.name,
                    previous_type=declared.type,
                    detail=(
                        f"column {declared.name} is declared in contract "
                        f"v{contract.version} but absent from the dataset"
                    ),
                )
            )
            continue
        if normalise_type(observed.type) != normalise_type(declared.type):
            changes.append(
                SchemaChange(
                    change_type="TYPE_CHANGE",
                    severity=schema_spec.severity_for("TYPE_CHANGE"),
                    column_name=declared.name,
                    previous_type=declared.type,
                    new_type=observed.type,
                    detail=(
                        f"column {declared.name} is {observed.type} but the contract "
                        f"declares {declared.type}"
                    ),
                )
            )
        if (
            schema_spec.check_nullability
            and not declared.nullable
            and observed.nullable
            and contract.source.type == "postgres"
        ):
            changes.append(
                SchemaChange(
                    change_type="BREAKING",
                    severity=schema_spec.severity_for("BREAKING"),
                    column_name=declared.name,
                    detail=(
                        f"column {declared.name} is declared NOT NULL in the contract "
                        "but is nullable in the dataset"
                    ),
                )
            )

    undeclared = sorted(set(current.names) - set(schema_spec.required_columns))
    for name in undeclared:
        observed = current.column(name)
        changes.append(
            SchemaChange(
                change_type="ADDITIVE",
                severity=schema_spec.severity_for("ADDITIVE"),
                column_name=name,
                new_type=observed.type if observed else None,
                detail=f"column {name} is present but not declared in the contract",
            )
        )
    return changes


def _snapshot_changes(
    previous: ObservedSchema, current: ObservedSchema, contract: Contract
) -> list[SchemaChange]:
    """Differences between the previous snapshot and the current one."""
    schema_spec = contract.schema
    changes: list[SchemaChange] = []

    removed = [name for name in previous.names if name not in set(current.names)]
    added = [name for name in current.names if name not in set(previous.names)]

    candidates = _rename_candidates(removed, added, previous, current)
    paired_old = {old for old, _, _ in candidates}
    paired_new = {new for _, new, _ in candidates}

    for old, new, confidence in candidates:
        old_column = previous.column(old)
        new_column = current.column(new)
        changes.append(
            SchemaChange(
                change_type="COLUMN_RENAME_CANDIDATE",
                severity=schema_spec.severity_for("COLUMN_RENAME_CANDIDATE"),
                column_name=new,
                previous_column=old,
                previous_type=old_column.type if old_column else None,
                new_type=new_column.type if new_column else None,
                confidence=confidence,
                is_confirmed=False,
                detail=(
                    f"{old} disappeared and {new} appeared with a compatible type in "
                    f"the same run (confidence {confidence}). A CANDIDATE only: a "
                    "rename and a drop-plus-add are indistinguishable from the "
                    "outside, so it is never recorded as confirmed"
                ),
            )
        )

    for name in removed:
        if name in paired_old:
            continue
        column = previous.column(name)
        changes.append(
            SchemaChange(
                change_type="COLUMN_REMOVAL",
                severity=schema_spec.severity_for("COLUMN_REMOVAL"),
                column_name=name,
                previous_type=column.type if column else None,
                detail=f"column {name} was present in the previous run and has gone",
            )
        )

    for name in added:
        if name in paired_new:
            continue
        column = current.column(name)
        changes.append(
            SchemaChange(
                change_type="ADDITIVE",
                severity=schema_spec.severity_for("ADDITIVE"),
                column_name=name,
                new_type=column.type if column else None,
                detail=f"column {name} is new since the previous run",
            )
        )

    for name in current.names:
        if name in added:
            continue
        before = previous.column(name)
        after = current.column(name)
        if before is None or after is None:
            continue
        if normalise_type(before.type) != normalise_type(after.type):
            changes.append(
                SchemaChange(
                    change_type="TYPE_CHANGE",
                    severity=schema_spec.severity_for("TYPE_CHANGE"),
                    column_name=name,
                    previous_type=before.type,
                    new_type=after.type,
                    detail=(
                        f"column {name} changed from {before.type} to {after.type} "
                        "since the previous run"
                    ),
                )
            )
    return changes


def classify(
    previous: ObservedSchema | None,
    current: ObservedSchema,
    contract: Contract,
) -> list[SchemaChange]:
    """Compare the contract and the previous snapshot against what is there now."""
    changes = _contract_changes(current, contract)

    if previous is None:
        LOGGER.info(
            "%s: first schema snapshot, nothing to diff against", contract.dataset
        )
    elif previous.fingerprint != current.fingerprint:
        changes.extend(_snapshot_changes(previous, current, contract))

    # De-duplicate: a contract-level finding and a snapshot-level finding can
    # describe the same change, and reporting it twice would double-count it in
    # the score and open two incidents for one problem.
    unique: dict[tuple, SchemaChange] = {}
    for change in changes:
        key = (change.change_type, change.column_name, change.previous_column)
        unique.setdefault(key, change)
    return list(unique.values())
