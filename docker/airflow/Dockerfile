# ===========================================================================
# Airflow image.
#
# Base: apache/airflow:3.3.1-python3.12
#   interpreter : /home/airflow/.local/bin/python (a venv owned by uid 50000)
#   AIRFLOW_HOME: /opt/airflow
#   already has : curl, git, apache-airflow-providers-openlineage 2.20.0
#
# Three separate installs, in order of how much they are allowed to touch:
#
#   1. airflow-extra.txt  - provider bumps, straight into Airflow's env.
#   2. platform.txt       - the platform's own libraries. Every pin that also
#                           appears in Airflow 3.3.1's constraint file is set
#                           to exactly the constrained value, so this install
#                           is purely additive (verified with a pip dry-run
#                           report diffed against the image's `pip freeze`).
#   3. dbt.txt            - into an ISOLATED venv at /opt/dbt-venv, because
#                           dbt requires pathspec<1.1 while Airflow pins
#                           pathspec==1.1.1 and uses it for .airflowignore.
# ===========================================================================
FROM apache/airflow:3.3.1-python3.12

USER root

# /opt is root-owned in the base image, so the dbt venv directory has to be
# created and handed to the airflow user before the unprivileged install.
# /opt/airflow/auth, /opt/airflow/artifacts and /opt/airflow/logs each have
# a NAMED VOLUME mounted over them by docker-compose. Docker creates a volume
# mounted at a path the image does not contain as root-owned 0755, so uid
# 50000 cannot write there - airflow-init failed with
# "PermissionError: [Errno 13] Permission denied: /opt/airflow/auth/passwords.json".
# A volume mounted over a path that DOES exist in the image inherits that
# path's ownership instead, so the directories are created and chowned here.
RUN mkdir -p /opt/dbt-venv /opt/airflow/platform /opt/airflow/auth \
             /opt/airflow/artifacts /opt/airflow/logs \
 && chown -R airflow:0 /opt/dbt-venv /opt/airflow/platform /opt/airflow/auth \
                       /opt/airflow/artifacts /opt/airflow/logs \
 && chmod -R g+w /opt/airflow/auth /opt/airflow/artifacts /opt/airflow/logs

USER airflow

# --- 1 + 2: Airflow's own environment ------------------------------------
COPY docker/requirements/airflow-extra.txt docker/requirements/platform.txt /tmp/requirements/
RUN pip install --no-cache-dir \
        -r /tmp/requirements/airflow-extra.txt \
        -r /tmp/requirements/platform.txt \
 && pip check

# --- 3: dbt, isolated ----------------------------------------------------
COPY docker/requirements/dbt.txt /tmp/requirements/dbt.txt
RUN python -m venv /opt/dbt-venv \
 && /opt/dbt-venv/bin/pip install --no-cache-dir --upgrade pip \
 && /opt/dbt-venv/bin/pip install --no-cache-dir -r /tmp/requirements/dbt.txt \
 && /opt/dbt-venv/bin/pip check \
 && /opt/dbt-venv/bin/dbt --version

# Prove the isolation held: Airflow's own pathspec must still be the
# constrained 1.1.x, and dbt must be invisible to Airflow's interpreter.
RUN python -c "import pathspec, sys; v=pathspec.__version__; print('airflow pathspec', v); sys.exit(0 if v.startswith('1.1') else 1)" \
 && python -c "import importlib.util, sys; sys.exit(1 if importlib.util.find_spec('dbt') else 0)" \
 && echo "dbt is correctly absent from Airflow's environment"

# --- Platform source -----------------------------------------------------
# Also bind-mounted by docker-compose; the COPY keeps the image standalone.
COPY --chown=airflow:0 src/       /opt/airflow/platform/src/
COPY --chown=airflow:0 contracts/ /opt/airflow/platform/contracts/
COPY --chown=airflow:0 dbt/       /opt/airflow/platform/dbt/
COPY --chown=airflow:0 sql/       /opt/airflow/platform/sql/

ENV PYTHONPATH=/opt/airflow/platform \
    DQ_HOME=/opt/airflow/platform \
    DBT_BIN=/opt/dbt-venv/bin/dbt \
    DBT_PROJECT_DIR=/opt/airflow/platform/dbt \
    DBT_PROFILES_DIR=/opt/airflow/platform/dbt
