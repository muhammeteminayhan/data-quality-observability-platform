# Security

This is a **local, single-machine portfolio platform**. It is built to demonstrate
data-quality engineering, not to be exposed to a network. This page states exactly
what it does, what it deliberately does not do, and what would have to change before
anything like it faced real traffic.

Everything below was verified against the running stack, not assumed.

---

## 1. Secrets

### Nothing secret is in git

| Control | How it is enforced |
|---|---|
| `.env` is ignored | `.gitignore` entry, verified with `git check-ignore .env` and a `git add -n .` dry run — not by trusting the glob |
| `.env.example` carries placeholders only | Every secret value is a literal `change_me_*`; there are 55 keys and 12 of them are secrets |
| The stack refuses placeholder secrets | `scripts/start.sh` greps `.env` for `change_me` and exits before building |
| Generated credentials never reach stdout | See *Airflow* below |

`git status` is not evidence that data and secrets are out of a commit; the only
evidence is what git would really stage. Re-run the dry run after any change to
`.gitignore`:

```bash
git add -n . | grep -E '[.]env$|^add .data/|[.]parquet$'   # must print nothing
```

### Secrets are read from the environment, never hardcoded

`src/common/config.py` builds every connection from environment variables and
fails fast when one is missing. There is no default password anywhere in `src/`.

The one place this was genuinely awkward is `sql/platform/00_init.sh`. A `.sql`
file in `/docker-entrypoint-initdb.d` cannot read environment variables, which
would mean committing the database passwords. It is therefore a shell script that
passes each password to `psql` as a variable.

Three forms were tested against PostgreSQL 18.7 and **only one works**:
`psql -c "... :'pw'"` does not interpolate, a dollar-quoted `DO $$ ... :'pw' ... $$`
block does not interpolate, and a plain stdin heredoc does. The comment in that
file records the measurement so nobody re-discovers it.

---

## 2. Database privileges

Three roles, one database each, created by `00_init.sh`:

| Role | Owns | Also granted |
|---|---|---|
| `OBSERVABILITY_DB_USER` | `observability_db` | `SELECT` on `platform_init_log` in `postgres`, so the platform can assert its own initialisation completed |
| `AIRFLOW_DB_USER` | `airflow_db` | nothing |
| `MARQUEZ_DB_USER` | `marquez_db` | nothing |

Marquez's bundled `marquez.dev.yml` **hardcodes** the database name, user and
password as `marquez` and substitutes only the host and port, so a custom config
file (`marquez/marquez.yml`) is required to give it its own role rather than
sharing one.

### Reads are read-only transactions

`src/common/db.py` opens a connection with `SET default_transaction_read_only = on`
unless the caller explicitly asks for write access:

```python
def source_connection(*, read_only: bool = True, **kwargs): ...
```

Every detector, profiler and validator reads the source this way. Only the data
generator and the fault injector pass `read_only=False`. That is a real guard: a
detector with a typo in a CTE cannot modify the data it is judging.

### The raw layer has almost no constraints — on purpose

`raw` carries no `NOT NULL`, `UNIQUE`, `FK` or `CHECK` on any business column.
The only key is `ingestion_id GENERATED ALWAYS AS IDENTITY`. This is the opposite
of a hardening recommendation and it is deliberate: the platform's job is to
**detect** bad data, so bad data has to be able to land. A duplicate source row
has to survive as a duplicate, or the uniqueness rule has nothing to find.

The constraints that matter are in `observability_db`, where they protect the
platform's own conclusions rather than the data under test. Four of them were
negative-tested:

| Attempt | Result |
|---|---|
| `quality_score` with `has_critical_failure = true` and `health_status = 'healthy'` | rejected, `ck_critical_is_not_healthy` |
| `gate_decision` with `critical_count = 1` and `decision = 'PASS'` | rejected, `ck_critical_fails_gate` |
| `incident` with `status = 'RESOLVED'` and `resolved_at` NULL | rejected, `ck_resolved_consistency` |
| two live incidents sharing a fingerprint | rejected, `ux_incidents_active_fingerprint` |

A bug in the scoring code cannot write "critical but healthy" into the database,
because the database refuses it.

---

## 3. Airflow

### SimpleAuthManager stores passwords in plaintext

Airflow 3.3.1 ships `SimpleAuthManager`, and its own source carries a warning that
it keeps passwords in plaintext **and prints generated ones to the logs**. That is
unacceptable even locally, because logs get pasted into issues.

`airflow/scripts/airflow_init.sh` therefore writes `$AIRFLOW_HOME/auth/passwords.json`
itself, from `.env`, as a flat `{username: password}` map, before the API server
starts. Airflow then finds a password file already present and generates nothing,
so no credential reaches stdout.

This is the right fix for a local stack and **the wrong authentication model for
anything else**. A deployment would replace the auth manager with one backed by an
identity provider.

### Other Airflow notes

* `AIRFLOW_FERNET_KEY` and `AIRFLOW_JWT_SECRET` come from `.env` and must be
  rotated if the file is ever shared. `.env.example` explains how to generate both.
* Airflow 3 needs a separate `dag-processor` process; the scheduler no longer
  parses DAGs. Running one fewer process is not a security saving, it is a broken
  deployment.
* The DAG writes `artifacts/` into a named volume (`dq_artifacts`), not into the
  repository.

---

## 4. The alert webhook

`src/webhook/receiver.py` listens on `ALERT_WEBHOOK_PORT` and accepts a shared
bearer token. All four paths were executed against the running service:

| Request | Response |
|---|---|
| valid token, new alert | `200 received` |
| valid token, byte-identical repeat | `200 duplicate`, with the original delivery id |
| wrong token | `401` |
| malformed body | `422`, recorded as `rejected` |

Two decisions worth stating:

**A duplicate returns 200, not 4xx.** A non-2xx makes the sender retry harder, so
answering a duplicate with an error turns one duplicate into a storm. The receiver
records it as a duplicate and returns success.

**A malformed body is recorded, not just refused.** A sender that starts posting
rubbish is itself an incident, so the rejection is stored with its reason.

The token is checked against **two** headers. Grafana's webhook contact point sends
credentials as `Authorization: <scheme> <credentials>`, not as an arbitrary custom
header, so the receiver accepts `Authorization: Bearer <token>` as well as its own
`X-DQ-Token`. That was found by watching Grafana fail to authenticate, not by
reading documentation.

This is a shared static token over plain HTTP on localhost. It is adequate for a
webhook between two containers on one bridge network and nothing more. Real alert
routing would use mTLS or a signed payload.

---

## 5. MinIO

`scripts/minio_init.sh` creates the bucket with the root credentials and then
creates a **separate service account** with a policy scoped to that bucket, which
is what the platform uses (`MINIO_ACCESS_KEY` / `MINIO_SECRET_KEY`). The root
credentials are never used by application code.

The least-privilege claim was negative-tested rather than assumed: the service
account was asked to create a second bucket (`mc mb`) and **was refused**. A policy
that has never been tested against an operation it should block is a policy nobody
knows the shape of.

---

## 6. Metrics cardinality

The Prometheus exporter (`src/metrics/collectors.py`) is a security as well as a
cost concern: an unbounded label set is a denial-of-service against your own
monitoring, and labels often leak identifiers.

Every label comes from a bounded, declared set: `dataset`, `dimension`, `severity`,
`change_type`, a contract-declared `column`, `status`, `decision`,
`timestamp_source`. There is **no** customer id, order id, run id or incident id,
and no error strings — an exception message is unbounded text and would turn one
new failure mode into a new time series forever.

Two deliberate omissions, both of which could be mistaken for bugs:

* a dataset with no usable timestamp emits **no** freshness series at all, because
  a zero would read as perfectly fresh;
* an `inconclusive` drift test publishes **no** statistic, because plotting a number
  the platform refuses to stand behind invites trust it has not earned.

---

## 7. Deliberately absent

Nothing here is an oversight; each line is a decision with a reason.

| Not present | Why | What production would need |
|---|---|---|
| TLS anywhere | Everything is on one Docker bridge network reachable only from the host | TLS between every hop, certificates managed outside the repo |
| Real identity | A local stack has one operator | OIDC/SAML for Airflow, Grafana and the API, with RBAC |
| A secret manager | `.env` is the simplest thing that keeps secrets out of git | Vault / AWS Secrets Manager / SOPS, with rotation |
| Rate limiting on the API and webhook | Both are localhost-only and the webhook is idempotent | Per-client limits and a request budget |
| Audit logging of reads | The platform records what it *concluded*, not who looked | An access log with identity attached |
| Network segmentation | Compose puts everything on one network | Separate subnets for data, control and observability planes |

### Do not expose this stack

The published host ports (listed in [OPERATIONS.md](OPERATIONS.md)) bind to all
interfaces, as Docker's default does. On a shared or untrusted network, bind them to
`127.0.0.1` by prefixing each published port in `docker-compose.yml`, or do not
publish them at all and reach the services through `docker compose exec`.

---

## 8. If you think a secret leaked

1. Stop the stack: `./scripts/stop.sh`.
2. Rotate every value in `.env`. They are independent, so rotate all of them rather
   than guessing which one leaked.
3. `docker compose down -v` destroys the volumes that hold the old credentials' data
   directories. PostgreSQL will **not** re-run its init scripts on a populated data
   directory, so changing a password in `.env` alone does not change that role's
   password in an existing volume.
4. Check whether the secret was ever committed:

```bash
git log --all --diff-filter=A --name-only | grep -i env
```

   If it was, rotating is not enough — the history has to be rewritten and the
   credential treated as public.
