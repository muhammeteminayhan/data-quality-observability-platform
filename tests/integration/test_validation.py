"""Great Expectations against the live databases, and the dbt layer."""

from __future__ import annotations

import pytest

pytestmark = [pytest.mark.integration]


def test_gx_validates_a_postgres_dataset_and_records_every_field(
    contracts_by_name, partition_date, test_run
):
    from src.validation.persist import persist
    from src.validation.runner import validate_contract

    contract = contracts_by_name["raw.orders"]
    outcome = validate_contract(contract, partition_date=partition_date)

    assert outcome.error_message is None, outcome.error_message
    assert len(outcome.outcomes) == len(contract.quality_rules)
    assert outcome.rows_evaluated and outcome.rows_evaluated > 1000
    assert outcome.engine_version

    # Every result must be attributable to the contract rule that produced
    # it, or it cannot be scored by dimension or acted on by the gate.
    declared = {rule.name for rule in contract.quality_rules}
    assert {o.rule_name for o in outcome.outcomes} == declared
    for result in outcome.outcomes:
        assert result.dimension in (
            "completeness", "uniqueness", "validity", "consistency", "freshness", "volume"
        )
        assert result.severity.value in ("INFO", "WARNING", "ERROR", "CRITICAL")

    validation_run_id = persist(outcome, run_id=test_run.run_id)
    assert validation_run_id > 0


def test_the_sql_rules_really_execute_across_tables(contracts_by_name, partition_date):
    from src.validation.runner import validate_contract

    contract = contracts_by_name["raw.orders"]
    outcome = validate_contract(contract, partition_date=partition_date)

    # These span tables, so no column expectation could state them. If
    # `{batch}` substitution or the joins were broken they would error rather
    # than pass, which is why the exception field is asserted too.
    cross_table = [
        "every_order_has_a_known_customer",
        "order_total_matches_line_items",
        "settled_payments_match_order_total",
    ]
    by_name = {o.rule_name: o for o in outcome.outcomes}
    for name in cross_table:
        assert name in by_name, f"{name} did not run at all"
        assert by_name[name].exception_message is None, by_name[name].exception_message


def test_gx_validates_a_parquet_dataset_through_a_file_datasource(
    contracts_by_name, partition_date
):
    from src.validation.runner import validate_contract

    contract = contracts_by_name["daily_sales"]
    assert contract.source.type == "parquet"
    outcome = validate_contract(contract, partition_date=partition_date)

    assert outcome.error_message is None, outcome.error_message
    assert outcome.rows_evaluated and outcome.rows_evaluated > 0


def test_the_contract_engine_evaluates_the_pandas_rules(contracts_by_name, partition_date):
    from src.contracts import engine

    contract = contracts_by_name["inventory_snapshots"]
    results = engine.evaluate(contract, partition_date=partition_date)

    # The pandas execution engine cannot run SQL, so these cross-column
    # identities are the contract engine's job.
    assert results, "expected at least one pandas_query rule"
    for result in results:
        assert result.expectation_type == "pandas_query"
        assert result.element_count and result.element_count > 0
        assert result.exception_message is None, result.exception_message


def test_a_broken_pandas_query_fails_rather_than_passing(contracts_by_name, partition_date):
    import dataclasses

    from src.common.severity import Severity
    from src.contracts import engine
    from src.contracts.models import QualityRule

    contract = contracts_by_name["inventory_snapshots"]
    broken = QualityRule(
        name="deliberately_broken",
        dimension="consistency",
        severity=Severity.ERROR,
        expectation="pandas_query",
        kwargs={"query": "this is not a valid query at all"},
    )
    patched = dataclasses.replace(contract, quality_rules=(broken,))
    results = engine.evaluate(patched, partition_date=partition_date)

    # A rule that cannot run is NOT a pass - a typo in a contract must never
    # silently retire a check.
    assert len(results) == 1
    assert results[0].success is False
    assert results[0].exception_message


def test_dbt_builds_and_its_artifacts_are_recorded(test_run):
    from src.dbt_runner.run import run_and_persist

    invocation, severity = run_and_persist(run_id=test_run.run_id, command="build")
    summary = invocation.summary
    assert summary is not None

    assert invocation.executed_cleanly, invocation.stderr[-500:]
    assert len(summary.models) == 12
    assert summary.models_success == len(summary.models)
    assert len(summary.tests) >= 80
    assert summary.tests_fail == 0
    assert summary.data_is_trustworthy is True

    # Model counts and test counts answer different questions and are kept
    # apart: 12 models executing says nothing about the data being right.
    assert summary.models_success != summary.tests_pass or len(summary.models) == len(
        summary.tests
    )
    generic = [n for n in summary.tests if n.test_type == "generic"]
    singular = [n for n in summary.tests if n.test_type == "singular"]
    assert generic and singular


def test_the_warehouse_reconciles_to_the_raw_source(source_db):
    from src.common.db import fetch_value

    # Two independent paths to the same figure. dbt exiting 0 proves the SQL
    # ran; this proves the output is right.
    raw_net = fetch_value(source_db, "SELECT round(sum(line_total), 2) FROM raw.order_items")
    agg_net = fetch_value(
        source_db, "SELECT round(sum(net_amount), 2) FROM analytics.agg_daily_sales"
    )
    assert raw_net == agg_net, f"raw {raw_net} vs analytics {agg_net}"

    settled_minus_refunded = fetch_value(
        source_db,
        """
        SELECT round(
            sum(CASE WHEN payment_type <> 'refund' THEN payment_value ELSE 0 END)
          - sum(CASE WHEN payment_type = 'refund' THEN -payment_value ELSE 0 END), 2)
        FROM raw.payments
        """,
    )
    fact_net = fetch_value(
        source_db, "SELECT round(sum(net_revenue), 2) FROM analytics.fct_orders"
    )
    assert settled_minus_refunded == fact_net


def test_staging_rejects_nothing_when_the_source_is_clean(source_db):
    from src.common.db import fetch_value

    rejected = fetch_value(source_db, "SELECT count(*) FROM staging.stg_rejected_rows")
    assert rejected == 0, "clean data should produce no rejects"


def test_refunds_survive_into_the_warehouse(source_db):
    from src.common.db import fetch_value

    raw_refunds = fetch_value(
        source_db, "SELECT count(*) FROM raw.payments WHERE payment_type='refund'"
    )
    flagged = fetch_value(
        source_db, "SELECT count(*) FROM analytics.fct_orders WHERE was_refunded"
    )
    assert raw_refunds > 0
    assert flagged == raw_refunds
