# Operations

How to run the platform, what it needs, and what to do when something goes wrong.
Every failure described here happened during development; none is hypothetical.

---

## 1. Requirements

| Need | Value | Note |
|---|---|---|
| Docker memory, full stack | **~10 GiB** | compose memory limits total ~8.3 GiB steady; leave headroom |
| Docker memory, reduced profile | **~6 GiB** | `./scripts/start.sh --dev` |
| Disk | ~6 GiB | images, two PostgreSQL volumes, MinIO, Grafana, Prometheus |
| Host ports | 11, listed below | all chosen outside this machine's WinNAT reserved ranges |

Measured on Docker Desktop (WSL2) with 20 CPUs and 15.48 GiB allocated.

On Windows, raise Docker's memory in Docker Desktop → Settings → Resources. The
usual symptom of too little is the Airflow scheduler or Marquez being OOM-killed
mid-run, which looks like a hang rather than an error.

---

## 2. Host ports

| Service | Host port | URL |
|---|---|---|
| postgres-source (`commerce_db`) | 5462 | `postgresql://localhost:5462/commerce_db` |
| postgres-platform (`observability_db`) | 5463 | `postgresql://localhost:5463/observability_db` |
| MinIO API / console | 9320 / 9321 | <http://localhost:9321> |
| Airflow | 8293 | <http://localhost:8293> |
| Marquez API / admin | 5055 / 5056 | <http://localhost:5055/api/v1/namespaces> |
| Marquez web | 3045 | <http://localhost:3045> |
| Prometheus | 9290 | <http://localhost:9290> |
| Grafana | 3046 | <http://localhost:3046> |
| Read API | 8294 | <http://localhost:8294/summary> |
| Metrics exporter | 9291 | <http://localhost:9291/metrics> |
| Alert webhook | 8295 | <http://localhost:8295/health> |

These are unusual numbers on purpose. Windows reserves TCP ranges for WinNAT
(`netsh int ipv4 show excludedportrange protocol=tcp`), and a port inside one of
them fails to bind with a misleading permissions error. The reserved set changes
across reboots, so **re-measure rather than reusing a list** if a bind fails.

Every port is also clear of the other projects in this portfolio series, so all
four stacks can run at once.

---

## 3. Start, stop, check

```bash
cp .env.example .env     # then replace every change_me value
./scripts/start.sh       # full stack
```

```bash
./scripts/start.sh --dev
```

The reduced profile runs the whole data-quality workflow — generation,
validation, the six detectors, scoring, incidents, the gate and dbt — with no
Marquez, Prometheus, Grafana, API, exporter or webhook. It also sets
`OPENLINEAGE_DISABLED=true`, because with Marquez absent every task would
otherwise log a failed emission.

```bash
./scripts/health_check.sh          # check once
./scripts/health_check.sh --wait   # wait for the stack to come up first
```

```bash
./scripts/stop.sh            # stop, keep all data
./scripts/stop.sh --purge    # also delete the volumes (asks for confirmation)
```

### Why `health_check.sh` exists at all

`docker compose ps` answers "is the process listening". It does not answer "did
the initialisation actually run", and the difference bit for real:

> A PostgreSQL init script failed. The entrypoint aborted, `restart: unless-stopped`
> brought the container back, `initdb` was skipped because the data directory
> already existed, the server started normally and **the healthcheck passed** — with
> no roles, no databases and no tables.

So `00_init.sh` writes a completion marker row into `platform_init_log` only after
every step has succeeded, and `health_check.sh` asserts that row exists. The only
recovery from the bad state is `./scripts/stop.sh --purge`, because PostgreSQL
never re-runs `/docker-entrypoint-initdb.d` on a populated data directory.

For the same reason every PostgreSQL healthcheck in `docker-compose.yml` uses
`pg_isready -h 127.0.0.1` rather than the unix socket: during `initdb` the
entrypoint runs a temporary server on the **socket only**, so a socket-based
check reports healthy before any init script has run.

---

## 4. Running the pipeline

### Through Airflow

Open <http://localhost:8293>, log in with `AIRFLOW_ADMIN_USER` /
`AIRFLOW_ADMIN_PASSWORD` from `.env`, and trigger
`centralized_data_quality_pipeline`. It is 17 tasks in 6 groups: `ingest`,
`validate`, `observe`, `quality_gate`, `transform`, `respond`.

To make it fail on purpose, trigger with a configuration:

```json
{"fault_profile": "missing_values"}
```

`python -m src.generator.cli --list` prints all 25 scenarios and 12 profiles.

### Without Airflow

Each stage has a CLI, which is also how the tests drive it:

```powershell
docker compose exec dq-api python -m src.generator.cli --date 2026-11-28
```

```powershell
docker compose exec dq-api python -m src.validation.cli --date 2026-11-28
```

```powershell
docker compose exec dq-api python -m src.observability.cli --date 2026-11-28
```

`src.observability.cli` is the full pass: register contracts, validate, freshness,
volume, schema diff, distribution drift, score, reconcile incidents, gate. It
exits non-zero when the gate fails.

---

## 5. Tests

```bash
./scripts/run_tests.sh unit          # no services needed
./scripts/run_tests.sh integration   # needs the stack up
./scripts/run_tests.sh e2e           # needs the stack up; mutates the source
./scripts/run_tests.sh all           # with coverage, written to artifacts/
```

The E2E scenarios inject real faults into `raw` and then restore it. They are
serial by construction and each resets first rather than trusting the previous
test to have cleaned up. Do not run them against anything you care about — which,
since the source is generated, is not a real risk here.

---

## 6. Troubleshooting

### PostgreSQL is healthy but the platform says nothing is initialised

The failure described in §3. Confirm it:

```powershell
docker compose exec postgres-platform psql -U postgres -c "TABLE platform_init_log"
```

No rows, or a missing table, means initialisation never completed. Fix:

```bash
./scripts/stop.sh --purge
./scripts/start.sh
```

### `airflow-init` dies with `PermissionError: /opt/airflow/auth/passwords.json`

A fresh named volume mounted at a path the image does **not** contain is created
root-owned `0755`, so uid 50000 cannot write to it. The Airflow image now
pre-creates `/opt/airflow/{auth,artifacts,logs}` owned by `airflow:0` so the
volumes inherit that ownership. If you see this after changing a volume mount,
remove the affected volume and start again.

### Airflow says the DAG was not found, but it imports fine

In Airflow 3, `airflow dags list` reads the metadata **database**, which the
`dag-processor` populates — it does not parse the folder. On a fresh deployment the
list is empty for a while. `airflow/scripts/airflow_init.sh` runs
`airflow dags reserialize` first for exactly this reason. To force it:

```powershell
docker compose exec airflow-scheduler airflow dags reserialize
```

### Grafana never becomes healthy

Grafana **refuses to boot** on invalid alerting provisioning — it is not a warning.
A `deleteContactPoints` block that removes a contact point a notification policy
references fails validation and the process exits with
`invalid service state: Failed ... starting module provisioning`. Check the logs:

```powershell
docker compose logs grafana --tail 50
```

Then fix `grafana/provisioning/alerting/*.yml`. Any provisioning error here is
fatal, not degraded.

### A Grafana panel is empty

An empty panel on a quality dashboard looks exactly like "no problems found",
which is the most dangerous failure this platform can have. Do not eyeball it:

`scripts/verify_dashboards.py` and `scripts/build_dashboards.py` run **on the
host**, not in a container: the first talks to Grafana on its published port and
both read `grafana/dashboards/`. They use only the Python standard library.

```powershell
$env:GRAFANA_ADMIN_USER = (Select-String '^GRAFANA_ADMIN_USER=' .env).Line.Split('=',2)[1]
```

```powershell
$env:GRAFANA_ADMIN_PASSWORD = (Select-String '^GRAFANA_ADMIN_PASSWORD=' .env).Line.Split('=',2)[1]
```

```powershell
python scripts/verify_dashboards.py
```

That runs **every** panel target through Grafana's `/api/ds/query` and classifies
each one OK / EMPTY / SHAPE / ERROR. A panel can be legitimately empty — for
example the live-incident panels when every incident has been resolved — and the
output lists those separately so each can be judged.

Set `GRAFANA_URL` if Grafana is not on the default `http://localhost:3046`.

Last verified run: **28 ok, 2 empty, 0 wrong shape, 0 errored**, the two empty
ones being `Live incidents by severity` and `Oldest live incident` with all 156
incidents resolved.

### Marquez logs a Flyway version warning

```
PostgreSQL 18.7 is newer than this version of Flyway and support has not been
tested. The latest supported version of PostgreSQL is 14.
```

Expected and harmless: all 91 migrations apply against PostgreSQL 18.7 and
`/api/v1/namespaces` answers. It is left unsilenced deliberately — a suppressed
warning is a warning nobody will see when it does start to matter.

### Marquez shows jobs but no datasets

Datasets live in namespaces named after their **physical location**, per the
OpenLineage naming spec, not in the DAG's `data-quality-platform` namespace,
which holds the jobs. Look in:

* `postgres://postgres-source:5432/commerce_db` — 16 datasets
* `s3://data-lake` — 3 partitioned datasets
* `postgres://postgres-platform:5432/observability_db` — 2

```powershell
curl http://localhost:5055/api/v1/namespaces
```

If a namespace genuinely holds no datasets, check the task logs for a dropped
emission. The emitter logs a warning and lets the task continue by design, so a
broken event does **not** fail the pipeline — it just means the lineage is
incomplete, and the log is the only place that says so.

### Volume checks all say `insufficient_history`

Correct on a fresh stack. The volume baseline is a rolling median with MAD and
needs at least 5 comparable history points (`same_day_type`, so weekdays are
compared with weekdays). Until then the status is `unknown` with method
`insufficient_history` — the platform does not invent a threshold.

Run the pipeline for several partition dates to build history. If it *stays*
unknown for many runs, that is the bug described in the README: `row_count` is a
measurement and `status` is a conclusion, and only the first belongs in a
baseline.

### dbt reports profiles.yml or dbt_project.yml "not found"

The file exists on the host but the container is reading the image's copy, baked
before the project existed. `dq-api` mounts `./dbt`, `./tests`, `./sql` and
`./contracts` for this reason. After adding or renaming a mount:

```bash
docker compose up -d --force-recreate dq-api
```

### Airflow starts behaving oddly after a dependency change

dbt lives in an **isolated venv** at `/opt/dbt-venv` inside the Airflow image.
`dbt-core` requires `pathspec<1.1`, while Airflow 3.3.1 pins `pathspec==1.1.1` and
uses it to evaluate `.airflowignore`. pip reports no conflict for that downgrade,
which is precisely why it has to be prevented rather than discovered later.

The Dockerfile asserts the isolation held — Airflow's `pathspec` must still be
`1.1.x` and `importlib.util.find_spec('dbt')` must be `None` in Airflow's
interpreter — so the **build** fails rather than the runtime. If that assertion
fires after you add a package, the package pulled dbt into the main environment.

### A port fails to bind on Windows

Windows reserves TCP ranges for WinNAT and a port inside one of them fails with a
permissions error rather than "in use". List the current ranges and pick outside
them:

```powershell
netsh int ipv4 show excludedportrange protocol=tcp
```

The reserved set changes across reboots. Measure it; never reuse an old list.

---

## 7. Where things are written

| What | Where | Lifetime |
|---|---|---|
| Every measurement, score, incident, gate decision | `observability_db` | volume `postgres_platform_data` |
| Generated source data | `commerce_db` schema `raw` | volume `postgres_source_data` |
| Parquet datasets | MinIO bucket `data-lake` | volume `minio_data` |
| dbt artifacts, coverage, GX state | volumes `dq_artifacts`, `gx_state` | survive `stop.sh`, deleted by `--purge` |
| Airflow logs | volume `airflow_logs` | — |

Nothing the pipeline produces is written into the repository working tree. `data/`
and `artifacts/` exist with `.gitkeep` files and are ignored.

---

## 8. Daily operation

There is no cron. The DAG is scheduled daily but a local stack is not left
running, so the realistic loop is:

1. `./scripts/start.sh`
2. trigger the DAG (or run `src.observability.cli` directly)
3. read Grafana at <http://localhost:3046>, or `curl localhost:8294/summary`
4. `./scripts/stop.sh` — keeping the volumes, so history accumulates across sessions

History accumulating is the point: the volume baseline, the drift reference and the
incident streaks are all comparisons against previous runs, and none of them means
anything on a single run.
