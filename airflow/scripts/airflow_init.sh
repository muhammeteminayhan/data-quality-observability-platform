#!/usr/bin/env bash
# ===========================================================================
# Airflow bootstrap (one-shot)
# ===========================================================================
# Entrypoint of the `airflow-init` service. Runs to completion and exits 0;
# the api-server, scheduler and dag-processor all wait on
# `service_completed_successfully`.
#
# Steps
#   1. wait for the metadata database
#   2. airflow db migrate
#   3. write the SimpleAuthManager password file from .env
#   4. register the connections the DAG uses
#   5. register the variables the DAG uses
#   6. verify the DAG actually imports
#
# Every step is idempotent, so the container can be re-run against an
# existing deployment.
# ===========================================================================
set -euo pipefail

log() { echo "[airflow-init] $*"; }

# ---------------------------------------------------------------------------
# 1. Wait for the metadata database.
# ---------------------------------------------------------------------------
log "waiting for the metadata database"
for attempt in $(seq 1 60); do
    if airflow db check >/dev/null 2>&1; then
        log "metadata database reachable"
        break
    fi
    if [ "${attempt}" -eq 60 ]; then
        log "ERROR: metadata database unreachable after 60 attempts"
        airflow db check || true
        exit 1
    fi
    sleep 2
done

# ---------------------------------------------------------------------------
# 2. Schema.
# ---------------------------------------------------------------------------
log "running airflow db migrate"
airflow db migrate

# ---------------------------------------------------------------------------
# 3. SimpleAuthManager password file.
# ---------------------------------------------------------------------------
# Airflow 3's default auth manager generates a random admin password on first
# boot and prints it to the logs. Writing the file here instead makes the
# password the one in .env, so the UI login is reproducible and the secret is
# never emitted to stdout.
#
# Format is a flat JSON object of username -> password.
AUTH_FILE="${AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE:-/opt/airflow/auth/passwords.json}"
mkdir -p "$(dirname "${AUTH_FILE}")"
python - "${AUTH_FILE}" <<'PY'
import json
import os
import sys

path = sys.argv[1]
user = os.environ["AIRFLOW_ADMIN_USER"]
password = os.environ["AIRFLOW_ADMIN_PASSWORD"]

try:
    with open(path, encoding="utf-8") as handle:
        existing = json.load(handle)
    if not isinstance(existing, dict):
        existing = {}
except (FileNotFoundError, json.JSONDecodeError):
    existing = {}

existing[user] = password

with open(path, "w", encoding="utf-8") as handle:
    json.dump(existing, handle)
    handle.write("\n")

print(f"[airflow-init] wrote credentials for '{user}' to {path}")
PY
chmod 600 "${AUTH_FILE}"

# ---------------------------------------------------------------------------
# 4. Connections.
# ---------------------------------------------------------------------------
# `connections add` fails if the conn_id exists, so each is deleted first.
# Deleting a missing connection is not an error worth stopping for.
add_pg_connection() {
    local conn_id="$1" host="$2" port="$3" schema="$4" login="$5" password="$6"
    airflow connections delete "${conn_id}" >/dev/null 2>&1 || true
    airflow connections add "${conn_id}" \
        --conn-type postgres \
        --conn-host "${host}" \
        --conn-port "${port}" \
        --conn-schema "${schema}" \
        --conn-login "${login}" \
        --conn-password "${password}" >/dev/null
    log "connection ${conn_id} -> ${host}:${port}/${schema}"
}

add_pg_connection postgres_source \
    "${SOURCE_POSTGRES_HOST}" "${SOURCE_POSTGRES_PORT}" "${SOURCE_POSTGRES_DB}" \
    "${SOURCE_POSTGRES_USER}" "${SOURCE_POSTGRES_PASSWORD}"

add_pg_connection postgres_observability \
    "${OBSERVABILITY_POSTGRES_HOST}" "${OBSERVABILITY_POSTGRES_PORT}" "${OBSERVABILITY_DB}" \
    "${OBSERVABILITY_DB_USER}" "${OBSERVABILITY_DB_PASSWORD}"

# MinIO, reachable over the S3 API. Stored as an aws connection so the
# standard provider tooling can use it as well as the platform code.
airflow connections delete minio_datalake >/dev/null 2>&1 || true
airflow connections add minio_datalake \
    --conn-type aws \
    --conn-login "${MINIO_ACCESS_KEY}" \
    --conn-password "${MINIO_SECRET_KEY}" \
    --conn-extra "{\"endpoint_url\": \"${MINIO_ENDPOINT}\", \"region_name\": \"us-east-1\"}" >/dev/null
log "connection minio_datalake -> ${MINIO_ENDPOINT}"

# The lineage backend, so the DAG's explicit emitter does not have to read
# raw environment variables.
airflow connections delete marquez_lineage >/dev/null 2>&1 || true
airflow connections add marquez_lineage \
    --conn-type http \
    --conn-host "${MARQUEZ_URL}" >/dev/null
log "connection marquez_lineage -> ${MARQUEZ_URL}"

# ---------------------------------------------------------------------------
# 5. Variables.
# ---------------------------------------------------------------------------
set_variable() {
    airflow variables set "$1" "$2" >/dev/null
    log "variable $1 = $2"
}

set_variable dq_contracts_dir    "${DQ_HOME}/contracts"
set_variable dq_artifacts_dir    "${DQ_ARTIFACTS_DIR}"
set_variable dq_default_fault_profile clean
set_variable dq_seed             "${DQ_SEED}"
set_variable dq_marquez_namespace "${MARQUEZ_NAMESPACE}"
set_variable dq_alert_webhook_url "${ALERT_WEBHOOK_URL}"
# Datasets whose CRITICAL failures must stop the pipeline. Read by the gate.
set_variable dq_blocking_datasets '["raw.orders", "raw.order_items", "raw.payments"]'

# ---------------------------------------------------------------------------
# 6. Verify the DAG imports.
# ---------------------------------------------------------------------------
# A DAG that fails to import shows up only as an absence in the UI, which is
# easy to miss. Failing here instead turns it into a startup error.
# In Airflow 3 `airflow dags list` reads the METADATA DATABASE, which the
# dag-processor populates - it does not parse the DAGs folder. On a fresh
# deployment the dag-processor has not run yet, so the list is empty and this
# check reported "expected DAG was not discovered" for a DAG that imports
# perfectly well. `reserialize` parses the folder and writes the result to the
# database, which is what makes the subsequent list meaningful.
log "parsing the DAGs folder (airflow dags reserialize)"
if ! airflow dags reserialize 2>/tmp/reserialize.err; then
    log "ERROR: DAG parsing failed"
    cat /tmp/reserialize.err
    exit 1
fi

log "checking DAG imports"
if ! airflow dags list --output json > /tmp/dags.json 2>/tmp/dags.err; then
    log "ERROR: 'airflow dags list' failed"
    cat /tmp/dags.err
    exit 1
fi

IMPORT_ERRORS="$(airflow dags list-import-errors --output json 2>/dev/null || echo '[]')"
if [ "${IMPORT_ERRORS}" != "[]" ] && [ -n "${IMPORT_ERRORS}" ]; then
    log "ERROR: DAG import errors present:"
    echo "${IMPORT_ERRORS}"
    exit 1
fi

python - <<'PY'
import json
import sys

with open("/tmp/dags.json", encoding="utf-8") as handle:
    dags = json.load(handle)

names = [d.get("dag_id") for d in dags]
print(f"[airflow-init] DAGs discovered: {names}")

required = "centralized_data_quality_pipeline"
if required not in names:
    print(f"[airflow-init] ERROR: expected DAG '{required}' was not discovered")
    sys.exit(1)
PY

log "done"
