#!/usr/bin/env bash
# ===========================================================================
# Run the test suite inside the platform container.
#
#   ./scripts/run_tests.sh              unit only (no services needed)
#   ./scripts/run_tests.sh integration  unit + integration (stack must be up)
#   ./scripts/run_tests.sh e2e          the end-to-end scenarios
#   ./scripts/run_tests.sh all          everything, with coverage
#
# The e2e scenarios RELOAD THE SOURCE DATA and inject faults, so they are not
# run by default - and they restore clean data afterwards.
# ===========================================================================
set -euo pipefail

cd "$(dirname "$0")/.."

MODE="${1:-unit}"
RUN=(docker compose run --rm)

case "$MODE" in
    unit)
        echo "[tests] unit"
        "${RUN[@]}" --no-deps dq-api python -m pytest tests/unit -v -p no:cacheprovider
        ;;
    integration)
        echo "[tests] unit + integration"
        "${RUN[@]}" dq-api python -m pytest tests/unit tests/integration -v -p no:cacheprovider
        ;;
    e2e)
        echo "[tests] end-to-end scenarios (these reload the source data)"
        "${RUN[@]}" dq-api python -m pytest tests/e2e -v -p no:cacheprovider
        ;;
    all)
        echo "[tests] everything, with coverage"
        "${RUN[@]}" dq-api python -m pytest tests \
            --cov=src --cov-report=term-missing --cov-report=xml:/opt/dq/artifacts/coverage.xml \
            -p no:cacheprovider
        ;;
    *)
        echo "usage: $0 [unit|integration|e2e|all]"
        exit 2
        ;;
esac
