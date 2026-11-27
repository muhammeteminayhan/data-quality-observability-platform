"""Schema fingerprinting and change classification."""

from __future__ import annotations

import pytest

from src.common.severity import Severity
from src.contracts.models import ColumnSpec, Contract, SchemaSpec, SourceSpec
from src.observability import schema_tracker
from src.observability.schema_tracker import ObservedColumn, ObservedSchema

pytestmark = pytest.mark.unit


def observed(*columns) -> ObservedSchema:
    return ObservedSchema(
        dataset="raw.test",
        columns=tuple(
            ObservedColumn(name=n, type=t, nullable=nu, position=i + 1)
            for i, (n, t, nu) in enumerate(columns)
        ),
    )


def contract(columns=(), **kwargs) -> Contract:
    return Contract(
        dataset="raw.test",
        version="1.0.0",
        source=SourceSpec(type="postgres", location="raw.test"),
        schema=SchemaSpec(
            enforce=True,
            columns=tuple(ColumnSpec(name=n, type=t, nullable=nu) for n, t, nu in columns),
            **kwargs,
        ),
    )


BASE = (("id", "bigint", False), ("name", "text", True), ("price", "numeric", True))


def test_fingerprint_is_stable_for_the_same_schema():
    assert observed(*BASE).fingerprint == observed(*BASE).fingerprint


def test_fingerprint_changes_when_a_type_changes():
    other = (("id", "bigint", False), ("name", "text", True), ("price", "text", True))
    assert observed(*BASE).fingerprint != observed(*other).fingerprint


def test_fingerprint_changes_when_columns_are_reordered():
    # A consumer reading by position would silently start reading the wrong
    # field, so a reordering is a real change.
    reordered = (("name", "text", True), ("id", "bigint", False), ("price", "numeric", True))
    assert observed(*BASE).fingerprint != observed(*reordered).fingerprint


def test_equivalent_type_names_do_not_count_as_a_change():
    a = observed(("ts", "timestamp with time zone", True))
    b = observed(("ts", "timestamptz", True))
    assert a.fingerprint == b.fingerprint


def test_pandas_str_dtype_is_treated_as_text():
    # pandas 3.0 reports a dedicated `str` dtype. Without this alias every
    # Parquet string column read as a CRITICAL type change.
    assert schema_tracker.normalise_type("str") == schema_tracker.normalise_type("text")
    assert schema_tracker.normalise_type("string") == schema_tracker.normalise_type("text")


def test_a_new_column_is_additive_and_only_info():
    before = observed(*BASE)
    after = observed(*BASE, ("extra", "text", True))
    changes = schema_tracker.classify(before, after, contract(columns=BASE))
    additive = [c for c in changes if c.change_type == "ADDITIVE"]
    assert [c.column_name for c in additive] == ["extra"]
    assert additive[0].severity is Severity.INFO


def test_a_type_change_is_critical():
    before = observed(*BASE)
    after = observed(("id", "bigint", False), ("name", "text", True), ("price", "text", True))
    changes = schema_tracker.classify(before, after, contract(columns=BASE))
    type_changes = [c for c in changes if c.change_type == "TYPE_CHANGE"]
    assert len(type_changes) == 1
    assert type_changes[0].column_name == "price"
    assert type_changes[0].previous_type == "numeric"
    assert type_changes[0].new_type == "text"
    assert type_changes[0].severity is Severity.CRITICAL


def test_a_dropped_column_the_contract_requires_is_critical():
    before = observed(*BASE)
    after = observed(("id", "bigint", False), ("name", "text", True))
    changes = schema_tracker.classify(before, after, contract(columns=BASE))
    removals = [c for c in changes if c.change_type == "COLUMN_REMOVAL"]
    assert "price" in [c.column_name for c in removals]
    assert all(c.severity is Severity.CRITICAL for c in removals)


def test_a_rename_is_only_ever_a_candidate():
    before = observed(*BASE)
    after = observed(
        ("id", "bigint", False), ("name", "text", True), ("amount", "numeric", True)
    )
    changes = schema_tracker.classify(before, after, contract(columns=BASE))
    candidates = [c for c in changes if c.change_type == "COLUMN_RENAME_CANDIDATE"]
    assert len(candidates) == 1
    candidate = candidates[0]
    assert candidate.previous_column == "price"
    assert candidate.column_name == "amount"
    # The whole point: never asserted as fact, and confidence never certainty.
    assert candidate.is_confirmed is False
    assert 0 < candidate.confidence <= 0.85


def test_rename_pairing_is_one_claim_per_column():
    # Two removals and two additions of the same type would naively produce
    # four candidate pairings for what may be a single rename.
    before = observed(("a", "text", True), ("b", "text", True))
    after = observed(("c", "text", True), ("d", "text", True))
    pairs = schema_tracker._rename_candidates(["a", "b"], ["c", "d"], before, after)
    assert len(pairs) == 2
    assert len({old for old, _, _ in pairs}) == 2
    assert len({new for _, new, _ in pairs}) == 2


def test_rename_candidates_need_a_matching_type():
    before = observed(("price", "numeric", True))
    after = observed(("label", "text", True))
    assert schema_tracker._rename_candidates(["price"], ["label"], before, after) == []


def test_a_column_missing_from_the_contract_is_caught_on_the_first_snapshot():
    # No previous snapshot at all: a dataset that never matched its contract
    # is broken now, not only once it changes.
    changes = schema_tracker.classify(None, observed(*BASE[:2]), contract(columns=BASE))
    assert [c.change_type for c in changes] == ["COLUMN_REMOVAL"]
    assert changes[0].column_name == "price"


def test_physical_nullability_is_not_compared_by_default():
    # The raw layer carries no NOT NULL constraints by design, so comparing
    # contract nullability against the DDL would report the platform's own
    # design as a BREAKING change on every run.
    spec_columns = (("id", "bigint", False),)
    after = observed(("id", "bigint", True))  # nullable in the database
    changes = schema_tracker.classify(None, after, contract(columns=spec_columns))
    assert [c for c in changes if c.change_type == "BREAKING"] == []


def test_nullability_is_compared_when_the_contract_opts_in():
    model = Contract(
        dataset="raw.test",
        version="1.0.0",
        source=SourceSpec(type="postgres", location="raw.test"),
        schema=SchemaSpec(
            enforce=True,
            check_nullability=True,
            columns=(ColumnSpec(name="id", type="bigint", nullable=False),),
        ),
    )
    changes = schema_tracker.classify(None, observed(("id", "bigint", True)), model)
    breaking = [c for c in changes if c.change_type == "BREAKING"]
    assert len(breaking) == 1
    assert breaking[0].severity is Severity.CRITICAL


def test_an_unchanged_schema_produces_no_changes():
    model = contract(columns=BASE)
    assert schema_tracker.classify(observed(*BASE), observed(*BASE), model) == []
