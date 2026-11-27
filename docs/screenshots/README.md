# Dashboard screenshots

**This directory deliberately contains no images.**

I could not capture the Grafana dashboards in the environment this project was
built in, and a data-quality platform is the last place to fake a screenshot. So
instead of an illustrative image, this directory holds
[panel-verification.txt](panel-verification.txt) — the real output of
`scripts/verify_dashboards.py` against the running stack, which is stronger
evidence than a picture anyway.

## Why there is no image

Grafana can render a panel to PNG server-side, but only with the
**grafana-image-renderer** plugin, which this stack does not install. That is a
considered omission: the plugin pulls in a headless Chromium, and a sibling
project in this series lost time to a `GF_RENDERING_SERVER_URL` misconfiguration
that leaves Grafana running but unable to render anything — failing silently at
the moment you need an image.

Capturing from a browser needs a browser driving the session, which was not
available here.

## What panel-verification.txt proves

Every panel target in all four dashboards was executed through Grafana's
`/api/ds/query` and classified:

```
panels: 28 ok, 2 empty, 0 wrong shape, 0 errored
```

The two empty panels are `Live incidents by severity` and `Oldest live incident`.
They were empty because at the time of the run all 156 incidents had status
`RESOLVED` — there were no live incidents to show. That is the correct rendering
of the truth, and the verifier lists such panels separately so each can be judged
rather than assumed.

This matters more than a screenshot would. A panel whose query is correct but
whose **result shape** is wrong renders empty, and an empty panel on a quality
dashboard looks exactly like "no problems found". A screenshot proves one moment
looked fine; the verifier proves every query returns data of the shape its panel
can draw.

## Capturing them yourself

The stack must be running (`./scripts/start.sh`).

1. Open <http://localhost:3046> and sign in with `GRAFANA_ADMIN_USER` /
   `GRAFANA_ADMIN_PASSWORD` from `.env`.

2. Run the pipeline several times so the time-series panels have history. One run
   is a single point, and the volume baseline says nothing until it has five
   comparable observations:

```powershell
docker compose exec dq-api python -m src.observability.cli --date 2026-11-28
```

3. For the incident panels to show live rows, inject a fault and run the pass
   again. `--mode fault` generates the data and then injects, so it needs
   `--truncate` for a clean starting point:

```powershell
docker compose exec dq-api python -m src.generator.cli --date 2026-11-28 --truncate --mode fault --fault-profile missing_values
```

4. Open each dashboard and capture it:

| File to save here | Dashboard |
|---|---|
| `01-platform-overview.png` | 1. Platform Overview |
| `02-freshness-and-volume.png` | 2. Freshness & Volume |
| `03-schema-and-distribution.png` | 3. Schema & Distribution |
| `04-incidents-and-reliability.png` | 4. Incidents & Reliability |

5. Re-run the verifier afterwards, so what the images show is also machine-checked
   rather than only looked at. It runs on the host, not in a container:

```powershell
python scripts/verify_dashboards.py
```

6. Restore clean data when you are done:

```powershell
docker compose exec dq-api python -m src.generator.cli --date 2026-11-28 --truncate
```

Note that the injected incidents will not close on their own. Resolution requires
**two** consecutive verified healthy passes, so run step 2 twice after step 6 to
watch the streak rule work: the first pass resolves nothing, the second resolves
everything.
