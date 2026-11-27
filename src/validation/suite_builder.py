"""Turn contract quality rules into a Great Expectations suite.

Dynamic by construction. The mapping from a contract's ``expectation:`` string
to a GX class is done by name conversion plus a lookup in
``great_expectations.expectations``, so a contract can use any of the 61
expectations GX ships without a line of code changing here. There is no
dictionary of supported rules to fall out of date.

Two rule shapes are not GX column expectations:

``unexpected_rows``
    Becomes ``UnexpectedRowsExpectation``, whose query fails the expectation
    if it returns any row. This is how referential integrity and the financial
    identities are expressed - they span tables, so no column expectation can
    state them.

``pandas_query``
    Evaluated by the contract engine against the Parquet frame, because the
    pandas execution engine cannot run SQL. Such rules are skipped here and
    reported by ``src/contracts/engine.py``.

Every expectation carries ``meta`` with the rule name, dimension and severity,
which is how a GX result is mapped back to the contract rule that produced it.
A result without that provenance could not be scored or gated.
"""

from __future__ import annotations

from typing import Any

import great_expectations.expectations as gxe
from great_expectations.core import ExpectationSuite

from src.common.logging import get_logger
from src.contracts.models import Contract, ContractError, QualityRule

LOGGER = get_logger(__name__)

# Rule kinds this module does not translate into GX expectations.
SQL_RULE = "unexpected_rows"
PANDAS_RULE = "pandas_query"
NON_GX_RULES = frozenset({PANDAS_RULE})


def expectation_class_name(expectation: str) -> str:
    """``expect_column_values_to_be_null`` -> ``ExpectColumnValuesToBeNull``."""
    return "".join(part.title() for part in expectation.split("_") if part)


def resolve_expectation_class(expectation: str) -> type:
    """Find the GX expectation class named by a contract."""
    if expectation == SQL_RULE:
        return gxe.UnexpectedRowsExpectation

    candidates = (expectation_class_name(expectation), expectation)
    for name in candidates:
        found = getattr(gxe, name, None)
        if found is not None:
            return found

    available = sorted(n for n in dir(gxe) if n.startswith("Expect"))
    raise ContractError(
        f"unknown expectation {expectation!r} (looked for class "
        f"{expectation_class_name(expectation)}); GX provides {len(available)} "
        "expectations, e.g. " + ", ".join(available[:5])
    )


def mostly_from(rule: QualityRule) -> float | None:
    """Translate a tolerated failure percentage into GX's ``mostly``.

    ``mostly`` is the fraction that must PASS, so a rule allowing 0.5 percent
    unexpected values becomes mostly=0.995. A zero tolerance returns None so
    the kwarg is left out entirely rather than passed as mostly=1.0, which
    some expectations treat differently from absent.
    """
    if rule.max_unexpected_percent <= 0:
        return None
    if not 0 < rule.max_unexpected_percent < 100:
        raise ContractError(
            f"{rule.name}: max_unexpected_percent must be between 0 and 100, "
            f"got {rule.max_unexpected_percent}"
        )
    return round(1.0 - (rule.max_unexpected_percent / 100.0), 6)


def build_expectation(rule: QualityRule, *, dataset: str) -> Any:
    """Instantiate one GX expectation from a contract rule."""
    expectation_class = resolve_expectation_class(rule.expectation)
    kwargs: dict[str, Any] = dict(rule.kwargs)

    if rule.expectation == SQL_RULE:
        if not rule.sql:
            raise ContractError(f"{rule.name}: an unexpected_rows rule needs `sql`")
        kwargs["unexpected_rows_query"] = rule.sql.strip()
    elif rule.column:
        kwargs["column"] = rule.column

    mostly = mostly_from(rule)
    if mostly is not None:
        kwargs["mostly"] = mostly

    # Provenance. Without this the result cannot be attributed to a rule, and
    # therefore cannot be scored by dimension or acted on by the gate.
    kwargs["meta"] = {
        "rule_name": rule.name,
        "dimension": rule.dimension,
        "severity": rule.severity.value,
        "dataset": dataset,
        "description": rule.description,
    }

    try:
        return expectation_class(**kwargs)
    except Exception as exc:
        raise ContractError(
            f"{dataset}/{rule.name}: cannot build {expectation_class.__name__} "
            f"with {sorted(kwargs)}: {exc}"
        ) from exc


def translatable_rules(contract: Contract) -> tuple[QualityRule, ...]:
    """Rules that become GX expectations."""
    return tuple(
        rule for rule in contract.quality_rules if rule.expectation not in NON_GX_RULES
    )


def engine_rules(contract: Contract) -> tuple[QualityRule, ...]:
    """Rules the contract engine evaluates itself."""
    return tuple(
        rule for rule in contract.quality_rules if rule.expectation in NON_GX_RULES
    )


def suite_name_for(contract: Contract) -> str:
    # The version is part of the name so a suite built from an older contract
    # is never silently reused after the contract changes.
    return f"{contract.dataset}__v{contract.version}"


def build_suite(contract: Contract) -> ExpectationSuite:
    """Build the expectation suite for a contract."""
    suite = ExpectationSuite(name=suite_name_for(contract))
    for rule in translatable_rules(contract):
        suite.add_expectation(build_expectation(rule, dataset=contract.dataset))

    skipped = engine_rules(contract)
    LOGGER.info(
        "suite %s: %d GX expectations, %d evaluated by the contract engine",
        suite.name,
        len(suite.expectations),
        len(skipped),
    )
    return suite
