#!/usr/bin/env bash
# ===========================================================================
# Assert the platform is genuinely working - not merely that the containers
# report healthy.
#
#   ./scripts/health_check.sh           check once
#   ./scripts/health_check.sh --wait    wait for everything to come up first
#
# Why this goes further than `docker compose ps`
# ----------------------------------------------
# A container healthcheck answers "is the process listening". It does not
# answer "did the initialisation actually run". This platform hit exactly
# that failure for real: a postgres init script failed, the entrypoint
# aborted, `restart: unless-stopped` brought the container back, initdb was
# skipped because the data directory already existed, the server started
# fine and the healthcheck PASSED - with no roles, no databases and no
# tables. So the checks below assert the end state, not the liveness.
# ===========================================================================
set -uo pipefail

cd "$(dirname "$0")/.."
set -a; . ./.env; set +a

PASS=0
FAIL=0

ok()   { echo "  [ok]   $1"; PASS=$((PASS + 1)); }
bad()  { echo "  [FAIL] $1"; FAIL=$((FAIL + 1)); }

if [ "${1:-}" = "--wait" ]; then
    echo "[health] waiting up to 5 minutes for container health"
    for _ in $(seq 1 60); do
        states="$(docker compose ps --format '{{.Health}}' 2>/dev/null | tr '\n' ' ')"
        case "$states" in
            *starting*) sleep 5 ;;
            *) break ;;
        esac
    done
fi

echo "[health] containers"
while read -r line; do
    name="${line%%:*}"
    state="${line##*:}"
    case "$state" in
        healthy|"") ok "$name ($([ -z "$state" ] && echo "no healthcheck" || echo "$state"))" ;;
        *) bad "$name is $state" ;;
    esac
done < <(docker compose ps --format '{{.Service}}:{{.Health}}' 2>/dev/null)

echo "[health] platform database initialisation"
# The marker row is written only after every step of 00_init.sh succeeded.
marker="$(docker compose exec -T postgres-platform psql -U "${PLATFORM_POSTGRES_USER}" \
    -d "${PLATFORM_POSTGRES_DB}" -tAc \
    "SELECT count(*) FROM platform_init_log" 2>/dev/null | tr -d ' \r')"
if [ "${marker:-0}" -ge 1 ]; then
    ok "platform_init_log has a completion marker"
else
    bad "platform_init_log is empty or missing - initialisation did NOT complete."
    bad "  Recovery: ./scripts/stop.sh --purge && ./scripts/start.sh"
fi

for db in "${OBSERVABILITY_DB}" "${AIRFLOW_DB}" "${MARQUEZ_DB}"; do
    exists="$(docker compose exec -T postgres-platform psql -U "${PLATFORM_POSTGRES_USER}" \
        -d "${PLATFORM_POSTGRES_DB}" -tAc \
        "SELECT 1 FROM pg_database WHERE datname='${db}'" 2>/dev/null | tr -d ' \r')"
    [ "$exists" = "1" ] && ok "database ${db} exists" || bad "database ${db} is missing"
done

tables="$(docker compose exec -T postgres-platform psql -U "${OBSERVABILITY_DB_USER}" \
    -d "${OBSERVABILITY_DB}" -tAc \
    "SELECT count(*) FROM information_schema.tables
     WHERE table_schema='public' AND table_type='BASE TABLE'" 2>/dev/null | tr -d ' \r')"
if [ "${tables:-0}" -ge 19 ]; then
    ok "observability_db has ${tables} tables"
else
    bad "observability_db has ${tables:-0} tables, expected at least 19"
fi

echo "[health] source warehouse"
raw="$(docker compose exec -T postgres-source psql -U "${SOURCE_POSTGRES_USER}" \
    -d "${SOURCE_POSTGRES_DB}" -tAc \
    "SELECT count(*) FROM information_schema.tables WHERE table_schema='raw'" 2>/dev/null | tr -d ' \r')"
[ "${raw:-0}" -eq 5 ] && ok "raw schema has 5 tables" || bad "raw schema has ${raw:-0} tables, expected 5"

echo "[health] object store"
if docker compose exec -T minio curl -fsS "http://127.0.0.1:9000/minio/health/live" >/dev/null 2>&1; then
    ok "MinIO is live"
else
    bad "MinIO is not answering its health endpoint"
fi

echo "[health] HTTP endpoints"
check_http() {
    local name="$1" url="$2"
    if curl -fsS -m 10 "$url" >/dev/null 2>&1; then
        ok "$name"
    else
        bad "$name did not answer ($url)"
    fi
}
check_http "Airflow api-server" "http://localhost:${AIRFLOW_HOST_PORT}/api/v2/version"
check_http "exporter /metrics"   "http://localhost:${DQ_EXPORTER_HOST_PORT}/metrics"
check_http "read API /health"    "http://localhost:${DQ_API_HOST_PORT}/health"
check_http "webhook /health"     "http://localhost:${ALERT_WEBHOOK_HOST_PORT}/health"
check_http "Prometheus"          "http://localhost:${PROMETHEUS_HOST_PORT}/-/healthy"
check_http "Grafana"             "http://localhost:${GRAFANA_HOST_PORT}/api/health"
check_http "Marquez admin"       "http://localhost:${MARQUEZ_HOST_ADMIN_PORT}/healthcheck"

echo "[health] Airflow DAG"
dags="$(docker compose exec -T airflow-scheduler airflow dags list --output json 2>/dev/null \
    | tr -d ' \r' | grep -c "centralized_data_quality_pipeline" || true)"
[ "${dags:-0}" -ge 1 ] && ok "DAG is registered" || bad "DAG is not registered"

echo
echo "========================================================"
echo "health check: ${PASS} passed, ${FAIL} failed"
[ "$FAIL" -eq 0 ] || exit 1
