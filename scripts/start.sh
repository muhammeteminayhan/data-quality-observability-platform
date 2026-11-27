#!/usr/bin/env bash
# ===========================================================================
# Bring the platform up.
#
#   ./scripts/start.sh            full stack  (needs ~10 GiB of Docker memory)
#   ./scripts/start.sh --dev      reduced profile (~6 GiB): the whole
#                                 data-quality workflow with no lineage,
#                                 metrics or dashboards
# ===========================================================================
set -euo pipefail

cd "$(dirname "$0")/.."

CORE_SERVICES=(
    postgres-source postgres-platform minio minio-init
    airflow-init airflow-apiserver airflow-scheduler airflow-dag-processor
)

if [ ! -f .env ]; then
    echo "ERROR: .env is missing. Create it from the template and replace every"
    echo "       change_me value:"
    echo "           cp .env.example .env"
    exit 1
fi

if grep -q "change_me" .env; then
    echo "ERROR: .env still contains change_me placeholders:"
    grep -n "change_me" .env | sed 's/^/    /'
    echo "Replace them before starting - the stack will not work with placeholders."
    exit 1
fi

DEV_MODE=false
if [ "${1:-}" = "--dev" ]; then
    DEV_MODE=true
fi

echo "[start] building images"
docker compose build

if [ "$DEV_MODE" = true ]; then
    echo "[start] reduced development profile: core services only"
    # OpenLineage is switched off because Marquez is not running in this
    # profile; left on, every task would log a failed emission.
    OPENLINEAGE_DISABLED=true COMPOSE_PROFILES= \
        docker compose up -d "${CORE_SERVICES[@]}"
else
    echo "[start] full stack"
    docker compose up -d
fi

echo "[start] waiting for health checks"
./scripts/health_check.sh --wait
