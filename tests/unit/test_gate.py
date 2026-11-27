"""The quality gate decision table."""

from __future__ import annotations

import pytest

from src.common.severity import Severity
from src.gates import gate as gate_module
from src.gates.gate import DatasetSignal

pytestmark = pytest.mark.unit

BLOCKING = ("raw.orders", "raw.payments")


def signal(dataset, severity, rules=()):
    return DatasetSignal(dataset=dataset, max_severity=severity, failed_rules=list(rules))


def decide(signals):
    return gate_module.evaluate(signals, blocking_datasets=BLOCKING)


def test_everything_clean_passes():
    decision = decide([signal("raw.orders", Severity.INFO), signal("raw.products", Severity.INFO)])
    assert decision.decision == "PASS"
    assert decision.passed is True
    assert decision.critical_count == 0


def test_a_warning_passes_with_warnings():
    decision = decide([signal("raw.products", Severity.WARNING, ["email_format"])])
    assert decision.decision == "PASS_WITH_WARNINGS"
    assert decision.passed is True
    assert decision.warning_count == 1


def test_a_critical_always_fails_even_on_a_non_blocking_dataset():
    decision = decide([signal("raw.products", Severity.CRITICAL, ["product_id_not_null"])])
    assert decision.decision == "FAIL"
    assert decision.passed is False
    assert decision.critical_count == 1
    assert "CRITICAL" in decision.reason


def test_an_error_on_a_blocking_dataset_fails():
    decision = decide([signal("raw.orders", Severity.ERROR, ["order_id_unique"])])
    assert decision.decision == "FAIL"
    assert "blocking dataset" in decision.reason


def test_an_error_on_a_non_blocking_dataset_only_warns():
    # An ERROR on a reference table should not stop the financial pipeline.
    decision = decide([signal("raw.products", Severity.ERROR, ["price_is_positive"])])
    assert decision.decision == "PASS_WITH_WARNINGS"
    assert decision.passed is True
    assert decision.error_count == 1


def test_the_worst_severity_decides():
    decision = decide(
        [
            signal("raw.products", Severity.WARNING),
            signal("raw.customers", Severity.INFO),
            signal("raw.orders", Severity.CRITICAL, ["total_amount_not_null"]),
        ]
    )
    assert decision.decision == "FAIL"
    assert decision.max_severity is Severity.CRITICAL


def test_blocked_tasks_are_recorded_only_on_a_failure():
    tasks = ["transform.dbt_build"]
    failed = gate_module.evaluate(
        [signal("raw.orders", Severity.CRITICAL)],
        blocking_datasets=BLOCKING,
        downstream_tasks=tasks,
    )
    assert failed.blocked_tasks == tasks

    passed = gate_module.evaluate(
        [signal("raw.orders", Severity.INFO)],
        blocking_datasets=BLOCKING,
        downstream_tasks=tasks,
    )
    assert passed.blocked_tasks == []


def test_an_empty_signal_list_passes():
    # Nothing measured is not a failure of the gate; the score reports the
    # absence of measurement separately.
    decision = decide([])
    assert decision.decision == "PASS"


def test_signal_from_parts_takes_the_worst_severity():
    built = gate_module.signal_from_parts(
        "raw.orders",
        [Severity.INFO, Severity.WARNING, Severity.ERROR],
        failed_rules=["a", "b"],
    )
    assert built.max_severity is Severity.ERROR
    assert built.failed_rules == ["a", "b"]


def test_quality_gate_failed_carries_the_decision():
    decision = decide([signal("raw.orders", Severity.CRITICAL)])
    error = gate_module.QualityGateFailed(decision)
    assert error.decision is decision
    assert str(error) == decision.reason
