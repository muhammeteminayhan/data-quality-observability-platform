"""Contract rules to Great Expectations, resolved dynamically."""

from __future__ import annotations

import pytest

from src.common.severity import Severity
from src.contracts.models import ContractError, QualityRule
from src.validation import suite_builder

pytestmark = pytest.mark.unit


def rule(**kwargs) -> QualityRule:
    base = {
        "name": "r",
        "dimension": "completeness",
        "severity": Severity.ERROR,
        "expectation": "expect_column_values_to_not_be_null",
        "column": "c",
    }
    return QualityRule(**(base | kwargs))


def test_snake_case_becomes_the_gx_class_name():
    assert (
        suite_builder.expectation_class_name("expect_column_values_to_not_be_null")
        == "ExpectColumnValuesToNotBeNull"
    )


def test_a_real_gx_class_is_resolved_without_any_lookup_table():
    found = suite_builder.resolve_expectation_class("expect_column_values_to_be_unique")
    assert found.__name__ == "ExpectColumnValuesToBeUnique"


def test_the_sql_rule_kind_maps_to_unexpected_rows():
    found = suite_builder.resolve_expectation_class("unexpected_rows")
    assert found.__name__ == "UnexpectedRowsExpectation"


def test_an_unknown_expectation_fails_with_a_useful_message():
    with pytest.raises(ContractError, match="unknown expectation"):
        suite_builder.resolve_expectation_class("expect_column_values_to_be_lovely")


def test_the_built_expectation_carries_its_provenance():
    built = suite_builder.build_expectation(
        rule(name="order_id_not_null", severity=Severity.CRITICAL), dataset="raw.orders"
    )
    meta = built.meta
    # Without this the GX result cannot be attributed to a contract rule, so
    # it could not be scored by dimension or acted on by the gate.
    assert meta["rule_name"] == "order_id_not_null"
    assert meta["dimension"] == "completeness"
    assert meta["severity"] == "CRITICAL"
    assert meta["dataset"] == "raw.orders"


def test_a_sql_rule_becomes_a_query_expectation():
    built = suite_builder.build_expectation(
        rule(
            name="ri",
            dimension="consistency",
            expectation="unexpected_rows",
            column=None,
            sql="SELECT 1 FROM {batch} WHERE false",
        ),
        dataset="raw.orders",
    )
    assert built.unexpected_rows_query.strip().startswith("SELECT 1")


def test_a_sql_rule_without_sql_is_rejected():
    with pytest.raises(ContractError, match="needs `sql`"):
        suite_builder.build_expectation(
            rule(expectation="unexpected_rows", column=None), dataset="raw.orders"
        )


def test_zero_tolerance_omits_mostly_entirely():
    assert suite_builder.mostly_from(rule(max_unexpected_percent=0.0)) is None


def test_a_tolerance_becomes_the_matching_mostly():
    # mostly is the fraction that must PASS, so 0.5% tolerated -> 0.995.
    assert suite_builder.mostly_from(rule(max_unexpected_percent=0.5)) == pytest.approx(0.995)


def test_an_impossible_tolerance_is_rejected():
    with pytest.raises(ContractError, match="between 0 and 100"):
        suite_builder.mostly_from(rule(max_unexpected_percent=150.0))


def test_pandas_query_rules_are_left_to_the_contract_engine(contracts_by_name):
    # The pandas execution engine cannot run SQL, so these rules are not GX
    # expectations at all and must not be translated.
    inventory = contracts_by_name["inventory_snapshots"]
    engine_rules = suite_builder.engine_rules(inventory)
    assert engine_rules, "inventory_snapshots should have a pandas_query rule"
    assert all(r.expectation == "pandas_query" for r in engine_rules)
    translated = suite_builder.translatable_rules(inventory)
    assert all(r.expectation != "pandas_query" for r in translated)


def test_the_suite_name_includes_the_contract_version(contracts_by_name):
    orders = contracts_by_name["raw.orders"]
    assert suite_builder.suite_name_for(orders) == f"raw.orders__v{orders.version}"


def test_a_whole_suite_is_built_from_a_real_contract(contracts_by_name):
    # ExpectationSuite resolves its owning project lazily, so GX raises
    # DataContextRequiredError without an active context. The runner always
    # creates one before building a suite; the test has to as well.
    import great_expectations as gx

    gx.get_context(mode="ephemeral")
    orders = contracts_by_name["raw.orders"]
    suite = suite_builder.build_suite(orders)
    assert len(suite.expectations) == len(suite_builder.translatable_rules(orders))
    assert all(e.meta.get("rule_name") for e in suite.expectations)
