# ===========================================================================
# Platform image - everything that is not Airflow.
#
# Runs: the read API (src.api), the Prometheus exporter (src.metrics), the
# alert webhook receiver (src.webhook) and the CLI entrypoints for the data
# generator, fault injector, contract validation and observability engine.
#
# No Airflow here, so dbt can live in the main environment with nothing to
# conflict with (see docker/requirements/dbt.txt).
# ===========================================================================
FROM python:3.12.16-slim-trixie

# Install dev/test extras by default so the suite can run inside the container.
ARG INSTALL_DEV=true

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONPATH=/opt/dq \
    DQ_HOME=/opt/dq

# curl is needed by the compose healthchecks. No compiler is required:
# psycopg[binary], pyarrow, numpy and scipy all ship manylinux wheels for
# CPython 3.12.
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
        curl \
        ca-certificates \
        tini \
        git \
 && rm -rf /var/lib/apt/lists/*

# ---------------------------------------------------------------------------
# Dependencies first, so editing source does not invalidate the pip layer.
# ---------------------------------------------------------------------------
COPY docker/requirements/ /tmp/requirements/
RUN if [ "$INSTALL_DEV" = "true" ]; then \
        pip install --no-cache-dir -r /tmp/requirements/dev.txt ; \
    else \
        pip install --no-cache-dir \
            -r /tmp/requirements/platform.txt \
            -r /tmp/requirements/dbt.txt ; \
    fi \
 && pip check \
 && rm -rf /tmp/requirements

# ---------------------------------------------------------------------------
# Source. Also bind-mounted by docker-compose so local edits take effect
# without a rebuild; the COPY keeps the image usable on its own.
# ---------------------------------------------------------------------------
WORKDIR /opt/dq
COPY src/        /opt/dq/src/
COPY contracts/  /opt/dq/contracts/
COPY dbt/        /opt/dq/dbt/
COPY sql/        /opt/dq/sql/
COPY tests/      /opt/dq/tests/
COPY pyproject.toml pytest.ini /opt/dq/

# Writable locations for the artifacts the platform produces.
RUN mkdir -p /opt/dq/artifacts /opt/dq/data /opt/dq/gx \
 && useradd --create-home --uid 10001 --shell /bin/bash dq \
 && chown -R dq:dq /opt/dq

USER dq

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["python", "-c", "print('platform image: pass a command, e.g. python -m src.generator.cli --help')"]
