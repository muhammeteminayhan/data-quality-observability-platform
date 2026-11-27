"""Environment-driven configuration for the whole platform.

Every setting comes from the environment, which docker-compose fills from the
git-ignored ``.env``. Nothing here carries a usable default for a credential:
a missing password raises when the settings are built rather than silently
connecting as some fallback user.

The platform runs in two different images (``docker/platform/Dockerfile`` and
``docker/airflow/Dockerfile``) and also on a developer's host for the unit
tests, so the same module has to work in all three places. That is why the
settings are plain dataclasses built from ``os.environ`` instead of anything
that assumes a particular working directory or a config file on disk.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class ConfigError(RuntimeError):
    """Raised when a required setting is absent or unusable."""


def _require(name: str) -> str:
    """Return a required environment variable or fail loudly.

    Validating at the boundary is the point: a typo in a compose file should
    surface as a named missing setting, not as an authentication failure
    several layers down.
    """
    value = os.environ.get(name)
    if value is None or value == "":
        raise ConfigError(f"required environment variable {name} is not set")
    return value


def _optional(name: str, default: str) -> str:
    value = os.environ.get(name)
    return default if value is None or value == "" else value


def _int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ConfigError(f"{name} must be an integer, got {raw!r}") from exc


# ---------------------------------------------------------------------------
# Database settings
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PostgresSettings:
    """Connection details for one PostgreSQL database."""

    host: str
    port: int
    database: str
    user: str
    password: str
    application_name: str = "dq-platform"

    @property
    def conninfo(self) -> str:
        """libpq connection string.

        Built with keyword=value pairs rather than a URL so a password
        containing a URL-significant character needs no escaping.
        """
        return " ".join(
            [
                f"host={self.host}",
                f"port={self.port}",
                f"dbname={self.database}",
                f"user={self.user}",
                f"password={self.password}",
                f"application_name={self.application_name}",
            ]
        )

    @property
    def sqlalchemy_url(self) -> str:
        """SQLAlchemy URL, used by Great Expectations' SQL datasource."""
        from urllib.parse import quote_plus

        return (
            f"postgresql+psycopg2://{quote_plus(self.user)}:{quote_plus(self.password)}"
            f"@{self.host}:{self.port}/{self.database}"
        )

    def redacted(self) -> str:
        """A form that is safe to log or embed in an error message."""
        return f"postgresql://{self.user}@{self.host}:{self.port}/{self.database}"


# ---------------------------------------------------------------------------
# Object storage
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ObjectStoreSettings:
    """MinIO / S3 settings for the Parquet datasets."""

    endpoint: str
    bucket: str
    access_key: str
    secret_key: str
    region: str = "us-east-1"

    def redacted(self) -> str:
        return f"{self.endpoint}/{self.bucket} as {self.access_key}"


# ---------------------------------------------------------------------------
# Lineage / alerting
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class LineageSettings:
    url: str
    namespace: str
    # Emission must never be the reason a data-quality task fails: lineage is
    # metadata about the run, not the run itself.
    timeout_seconds: float = 10.0
    enabled: bool = True


@dataclass(frozen=True)
class AlertSettings:
    webhook_url: str
    webhook_token: str
    # Minutes a fingerprint stays suppressed after an alert is sent.
    cooldown_minutes: int = 30


# ---------------------------------------------------------------------------
# Root settings
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Settings:
    """Everything the platform needs, resolved once."""

    environment: str
    log_level: str
    seed: int
    home: Path
    artifacts_dir: Path
    contracts_dir: Path
    source: PostgresSettings
    observability: PostgresSettings
    object_store: ObjectStoreSettings
    lineage: LineageSettings
    alerting: AlertSettings
    dbt_bin: str
    dbt_project_dir: Path
    dbt_profiles_dir: Path
    exporter_port: int
    platform_version: str = "1.0.0"
    # Datasets whose CRITICAL findings must stop the pipeline. Overridable so
    # the gate's behaviour is configuration, not a constant in the code.
    blocking_datasets: tuple[str, ...] = field(default_factory=tuple)


def _build_settings() -> Settings:
    home = Path(_optional("DQ_HOME", str(Path(__file__).resolve().parents[2])))

    source = PostgresSettings(
        host=_require("SOURCE_POSTGRES_HOST"),
        port=_int("SOURCE_POSTGRES_PORT", 5432),
        database=_require("SOURCE_POSTGRES_DB"),
        user=_require("SOURCE_POSTGRES_USER"),
        password=_require("SOURCE_POSTGRES_PASSWORD"),
        application_name="dq-platform-source",
    )

    observability = PostgresSettings(
        host=_require("OBSERVABILITY_POSTGRES_HOST"),
        port=_int("OBSERVABILITY_POSTGRES_PORT", 5432),
        database=_require("OBSERVABILITY_DB"),
        user=_require("OBSERVABILITY_DB_USER"),
        password=_require("OBSERVABILITY_DB_PASSWORD"),
        application_name="dq-platform-observability",
    )

    object_store = ObjectStoreSettings(
        endpoint=_require("MINIO_ENDPOINT"),
        bucket=_require("MINIO_BUCKET"),
        access_key=_require("MINIO_ACCESS_KEY"),
        secret_key=_require("MINIO_SECRET_KEY"),
    )

    lineage = LineageSettings(
        url=_optional("MARQUEZ_URL", "http://marquez:5000"),
        namespace=_optional("MARQUEZ_NAMESPACE", "data-quality-platform"),
        timeout_seconds=float(_optional("MARQUEZ_TIMEOUT_SECONDS", "10")),
        enabled=_optional("DQ_LINEAGE_ENABLED", "true").lower() != "false",
    )

    alerting = AlertSettings(
        webhook_url=_optional("ALERT_WEBHOOK_URL", "http://alert-webhook:8000/alerts"),
        webhook_token=_optional("ALERT_WEBHOOK_TOKEN", ""),
        cooldown_minutes=_int("DQ_INCIDENT_COOLDOWN_MINUTES", 30),
    )

    blocking = _optional(
        "DQ_BLOCKING_DATASETS", "raw.orders,raw.order_items,raw.payments"
    )

    return Settings(
        environment=_optional("DQ_ENV", "local"),
        log_level=_optional("DQ_LOG_LEVEL", "INFO").upper(),
        seed=_int("DQ_SEED", 20240115),
        home=home,
        artifacts_dir=Path(_optional("DQ_ARTIFACTS_DIR", str(home / "artifacts"))),
        contracts_dir=Path(_optional("DQ_CONTRACTS_DIR", str(home / "contracts"))),
        source=source,
        observability=observability,
        object_store=object_store,
        lineage=lineage,
        alerting=alerting,
        dbt_bin=_optional("DBT_BIN", "dbt"),
        dbt_project_dir=Path(_optional("DBT_PROJECT_DIR", str(home / "dbt"))),
        dbt_profiles_dir=Path(_optional("DBT_PROFILES_DIR", str(home / "dbt"))),
        exporter_port=_int("DQ_EXPORTER_PORT", 9091),
        blocking_datasets=tuple(
            name.strip() for name in blocking.split(",") if name.strip()
        ),
    )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Resolved settings, cached for the process.

    Cached rather than evaluated at module level so importing this module
    never requires a full environment - the unit tests can import helpers
    without a database being configured.
    """
    return _build_settings()


def reset_settings_cache() -> None:
    """Drop the cache. Used by tests that patch the environment."""
    get_settings.cache_clear()
