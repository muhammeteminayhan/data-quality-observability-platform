"""Contract loading: strictness, checksums and the real files."""

from __future__ import annotations

import pytest
import yaml

from src.contracts.loader import load_contracts, normalised_checksum, parse_contract
from src.contracts.models import ContractError

pytestmark = pytest.mark.unit

MINIMAL = {
    "dataset": "raw.thing",
    "version": "1.0.0",
    "source": {"type": "postgres", "location": "raw.thing", "ingestion_timestamp_column": "_ingested_at"},
}


def test_a_minimal_contract_parses():
    contract = parse_contract(dict(MINIMAL))
    assert contract.dataset == "raw.thing"
    assert contract.source.type == "postgres"
    assert contract.content_sha256


def test_an_unknown_top_level_key_is_rejected():
    payload = dict(MINIMAL) | {"freshnes": {"enabled": True}}
    with pytest.raises(ContractError, match="unknown key"):
        parse_contract(payload)


def test_an_unknown_key_inside_a_section_is_rejected():
    payload = dict(MINIMAL) | {"volume": {"enabled": True, "min_row": 5}}
    with pytest.raises(ContractError, match="unknown key"):
        parse_contract(payload)


def test_a_misspelled_severity_is_rejected_not_downgraded():
    payload = dict(MINIMAL) | {
        "quality_rules": [
            {
                "name": "r",
                "dimension": "completeness",
                "severity": "criticial",
                "expectation": "expect_column_values_to_not_be_null",
                "column": "c",
            }
        ]
    }
    with pytest.raises(ValueError, match="unknown severity"):
        parse_contract(payload)


def test_a_dimension_outside_the_six_is_rejected():
    payload = dict(MINIMAL) | {
        "quality_rules": [
            {
                "name": "r",
                "dimension": "timeliness",
                "severity": "ERROR",
                "expectation": "expect_column_values_to_not_be_null",
                "column": "c",
            }
        ]
    }
    with pytest.raises(ContractError, match="dimension must be one of"):
        parse_contract(payload)


def test_duplicate_rule_names_are_rejected():
    rule = {
        "name": "same",
        "dimension": "completeness",
        "severity": "ERROR",
        "expectation": "expect_column_values_to_not_be_null",
        "column": "c",
    }
    payload = dict(MINIMAL) | {"quality_rules": [rule, dict(rule)]}
    with pytest.raises(ContractError, match="duplicate rule name"):
        parse_contract(payload)


def test_a_rule_cannot_have_both_sql_and_column():
    payload = dict(MINIMAL) | {
        "quality_rules": [
            {
                "name": "r",
                "dimension": "consistency",
                "severity": "ERROR",
                "expectation": "unexpected_rows",
                "column": "c",
                "sql": "SELECT 1",
            }
        ]
    }
    with pytest.raises(ContractError, match="either `sql` or `column`"):
        parse_contract(payload)


def test_weights_that_do_not_sum_to_one_are_rejected():
    payload = dict(MINIMAL) | {"score": {"weights": {"completeness": 0.5, "validity": 0.2}}}
    with pytest.raises(ContractError, match="must sum to 1.0"):
        parse_contract(payload)


def test_critical_z_below_warning_z_is_rejected():
    payload = dict(MINIMAL) | {
        "volume": {"baseline": {"warning_z": 6.0, "critical_z": 3.0}}
    }
    with pytest.raises(ContractError, match="critical_z must not be below warning_z"):
        parse_contract(payload)


def test_freshness_without_any_timestamp_column_is_rejected():
    payload = {
        "dataset": "raw.thing",
        "version": "1.0.0",
        "source": {"type": "postgres", "location": "raw.thing"},
        "freshness": {"enabled": True, "warning_after_minutes": 10},
    }
    with pytest.raises(ContractError, match="neither"):
        parse_contract(payload)


def test_a_drift_column_absent_from_the_schema_is_rejected():
    payload = dict(MINIMAL) | {
        "schema": {"columns": [{"name": "a", "type": "text"}]},
        "distribution": {"enabled": True, "numeric": [{"column": "missing"}]},
    }
    with pytest.raises(ContractError, match="not in the declared schema"):
        parse_contract(payload)


def test_an_unimplemented_drift_method_is_rejected():
    payload = dict(MINIMAL) | {
        "distribution": {"enabled": True, "numeric": [{"column": "a", "method": "ks"}]}
    }
    with pytest.raises(ContractError, match="only psi is implemented"):
        parse_contract(payload)


def test_the_checksum_ignores_formatting_but_not_content():
    base = dict(MINIMAL)
    reordered = {key: base[key] for key in reversed(list(base))}
    assert normalised_checksum(base) == normalised_checksum(reordered)

    changed = dict(MINIMAL) | {"criticality": "critical"}
    assert normalised_checksum(base) != normalised_checksum(changed)


def test_checksum_catches_an_edit_without_a_version_bump():
    # Editing a threshold while leaving `version` alone must still register
    # as different content.
    a = dict(MINIMAL) | {"volume": {"min_rows": 100}}
    b = dict(MINIMAL) | {"volume": {"min_rows": 200}}
    assert a["version"] == b["version"]
    assert normalised_checksum(a) != normalised_checksum(b)


# --- the real contract files ---------------------------------------------


def test_every_shipped_contract_loads(contracts):
    assert len(contracts) == 8
    names = {contract.dataset for contract in contracts}
    assert names == {
        "raw.customers",
        "raw.products",
        "raw.orders",
        "raw.order_items",
        "raw.payments",
        "daily_sales",
        "inventory_snapshots",
        "customer_activity",
    }


def test_the_shipped_contracts_declare_rules_across_the_dimensions(contracts):
    total = sum(len(contract.quality_rules) for contract in contracts)
    assert total >= 60, f"expected a substantial rule set, got {total}"

    seen = {rule.dimension for contract in contracts for rule in contract.quality_rules}
    # Freshness and volume are measured by their own detectors rather than by
    # contract rules, so the rules cover the other four dimensions.
    assert {"completeness", "uniqueness", "validity", "consistency"} <= seen


def test_every_critical_dataset_has_at_least_one_critical_rule(contracts):
    for contract in contracts:
        if contract.criticality == "critical":
            assert contract.has_critical_rules, contract.dataset


def test_payments_does_not_require_a_positive_value(contracts_by_name):
    # A refund is a legitimate negative payment, so a blanket
    # "payment_value > 0" rule would fire on correct data.
    payments = contracts_by_name["raw.payments"]
    rule = payments.rule("payment_value_within_plausible_range")
    assert rule is not None
    assert rule.kwargs["min_value"] < 0


def test_loading_a_directory_with_no_contracts_fails_loudly(tmp_path):
    with pytest.raises(ContractError, match="no contract files found"):
        load_contracts(str(tmp_path))


def test_two_files_declaring_the_same_dataset_are_rejected(tmp_path):
    for name in ("a.yml", "b.yml"):
        (tmp_path / name).write_text(yaml.safe_dump(MINIMAL), encoding="utf-8")
    with pytest.raises(ContractError, match="same dataset"):
        load_contracts(str(tmp_path))
