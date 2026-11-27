#!/usr/bin/env bash
# ===========================================================================
# Stop the platform.
#
#   ./scripts/stop.sh             stop the containers, KEEP the data
#   ./scripts/stop.sh --purge     also delete the volumes
#
# --purge is the only way to re-run a first-time initialisation: postgres
# skips /docker-entrypoint-initdb.d entirely once its data directory exists,
# so a failed init can only be repaired by removing the volume.
# ===========================================================================
set -euo pipefail

cd "$(dirname "$0")/.."

if [ "${1:-}" = "--purge" ]; then
    echo "[stop] removing containers AND volumes - all generated data and all"
    echo "       recorded measurements will be lost."
    read -r -p "Type 'purge' to confirm: " answer
    if [ "$answer" != "purge" ]; then
        echo "[stop] cancelled"
        exit 1
    fi
    docker compose down -v --remove-orphans
    echo "[stop] done; the next start will re-run every initialisation script"
else
    docker compose down --remove-orphans
    echo "[stop] containers stopped; volumes kept"
fi
