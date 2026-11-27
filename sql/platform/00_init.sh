#!/usr/bin/env bash
# ===========================================================================
# postgres-platform : first-initialisation script
# ===========================================================================
# Runs once, from /docker-entrypoint-initdb.d, as the superuser over the local
# unix socket. Creates the three databases the platform needs plus the role
# that owns each one, then applies the observability schema.
#
# This is a shell script rather than plain SQL so the passwords can come from
# the environment: a .sql file in initdb.d cannot read env vars, which would
# mean committing credentials to the repository.
#
# The schema file is NOT mounted into /docker-entrypoint-initdb.d. If it were,
# the entrypoint would also execute it against POSTGRES_DB and create the
# observability tables in the wrong database. It is mounted at
# /opt/platform-sql/ and applied explicitly below, against observability_db.
#
# During first initialisation the server listens on the unix socket only
# (the entrypoint sets listen_addresses=''), so no --host is given here.
# ===========================================================================
set -euo pipefail

log() { echo "[platform-init] $*"; }

psql_super() {
    psql -v ON_ERROR_STOP=1 --no-psqlrc --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" "$@"
}

role_exists() {
    [ "$(psql_super -tAc "SELECT 1 FROM pg_roles WHERE rolname = '$1'")" = "1" ]
}

database_exists() {
    [ "$(psql_super -tAc "SELECT 1 FROM pg_database WHERE datname = '$1'")" = "1" ]
}

create_role() {
    local role="$1" password="$2"
    if role_exists "$role"; then
        log "role $role already exists"
    else
        # The password is passed as a psql variable so it is quoted by psql
        # rather than pasted into the DDL text by the shell.
        #
        # Three forms were tested against postgres 18.7 and only one works:
        #
        #   psql -c "... :'pw'"            -> NOT interpolated. psql sends the
        #                                     string straight to the server,
        #                                     which fails on the colon.
        #   DO $$ ... :'pw' ... $$         -> NOT interpolated. psql treats a
        #                                     dollar-quoted body as opaque.
        #   heredoc on stdin, no $$        -> interpolated correctly.
        #
        # Hence a plain stdin heredoc. The role name is a fixed value from
        # .env and is double-quoted as an identifier.
        psql_super --set=pw="$password" <<SQL
CREATE ROLE "$role" LOGIN PASSWORD :'pw';
SQL
        log "created role $role"
    fi
}

create_database() {
    local db="$1" owner="$2"
    if database_exists "$db"; then
        log "database $db already exists"
    else
        # CREATE DATABASE cannot run inside a transaction or a DO block.
        psql_super -c "CREATE DATABASE \"$db\" OWNER \"$owner\""
        log "created database $db owned by $owner"
    fi
}

log "creating platform roles"
create_role "${OBSERVABILITY_DB_USER}" "${OBSERVABILITY_DB_PASSWORD}"
create_role "${AIRFLOW_DB_USER}"       "${AIRFLOW_DB_PASSWORD}"
create_role "${MARQUEZ_DB_USER}"       "${MARQUEZ_DB_PASSWORD}"

log "creating platform databases"
create_database "${OBSERVABILITY_DB}" "${OBSERVABILITY_DB_USER}"
create_database "${AIRFLOW_DB}"       "${AIRFLOW_DB_USER}"
create_database "${MARQUEZ_DB}"       "${MARQUEZ_DB_USER}"

# ---------------------------------------------------------------------------
# Observability schema, applied as the owning role so every object belongs to
# that role rather than to the superuser.
# ---------------------------------------------------------------------------
log "applying observability schema to ${OBSERVABILITY_DB}"
psql -v ON_ERROR_STOP=1 --no-psqlrc \
    --username "${OBSERVABILITY_DB_USER}" \
    --dbname "${OBSERVABILITY_DB}" \
    --file /opt/platform-sql/10_observability_schema.sql

# Sanity check: the schema must have produced the expected number of tables.
TABLE_COUNT="$(psql -tAc \
    "SELECT count(*) FROM information_schema.tables
     WHERE table_schema = 'public' AND table_type = 'BASE TABLE'" \
    --username "${OBSERVABILITY_DB_USER}" --dbname "${OBSERVABILITY_DB}")"
log "observability_db now has ${TABLE_COUNT} tables"
if [ "${TABLE_COUNT}" -lt 19 ]; then
    log "ERROR: expected at least 19 observability tables, found ${TABLE_COUNT}"
    exit 1
fi

# Marquez manages its own schema with Flyway on first boot and Airflow runs
# `airflow db migrate`, so neither needs DDL from here.

# ---------------------------------------------------------------------------
# Completion marker
# ---------------------------------------------------------------------------
# Why this exists
# ---------------
# If this script fails on a first start, the postgres entrypoint aborts but
# the data directory has already been initialised. `restart: unless-stopped`
# then brings the container back, the entrypoint logs "Database directory
# appears to contain a database; Skipping initialization", the server starts
# normally and the healthcheck PASSES - leaving a server that reports healthy
# while having none of the roles, databases or tables.
#
# That failure mode was hit for real during development (a psql variable was
# not interpolated inside a dollar-quoted block), and a passing healthcheck
# hid it. The marker below is written only when every step above succeeded,
# so scripts/health_check.sh can assert that initialisation actually ran
# instead of trusting the container's health status.
#
# Recovery from a failed first init is a volume wipe - there is no way to
# re-trigger initdb on a populated data directory:
#     docker compose down -v
psql_super <<'SQL'
CREATE TABLE IF NOT EXISTS platform_init_log (
    id          integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    completed_at timestamptz NOT NULL DEFAULT now(),
    detail      text NOT NULL
);
SQL
psql_super --set=detail="observability_db=${TABLE_COUNT} tables; databases=${OBSERVABILITY_DB},${AIRFLOW_DB},${MARQUEZ_DB}" <<'SQL'
INSERT INTO platform_init_log (detail) VALUES (:'detail');
SQL
# The observability role needs to READ this marker so the platform can
# assert its own initialisation completed. SELECT only - the marker is
# written once, here, by the superuser.
psql_super -c "GRANT SELECT ON platform_init_log TO \"${OBSERVABILITY_DB_USER}\""
psql_super -c "GRANT CONNECT ON DATABASE \"${POSTGRES_DB}\" TO \"${OBSERVABILITY_DB_USER}\""
log "wrote completion marker to platform_init_log"

log "done"
