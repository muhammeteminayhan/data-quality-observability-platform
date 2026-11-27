"""MinIO / S3 access for the Parquet datasets.

MinIO speaks the S3 API, so boto3 is used with an explicit ``endpoint_url``.
Paths follow a Hive-style partition layout:

    s3://<bucket>/<dataset>/dt=YYYY-MM-DD/part-0.parquet

The dataset prefixes are ``daily_sales``, ``inventory_snapshots`` and
``customer_activity``.

``object_last_modified`` exists for the freshness detector: when a Parquet
dataset carries no usable event timestamp, the object's own mtime is the only
remaining signal, and the detector records which of the two it used rather
than presenting them as equivalent.
"""

from __future__ import annotations

import io
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from functools import lru_cache
from typing import TYPE_CHECKING, Any

import boto3
from botocore.client import Config as BotoConfig
from botocore.exceptions import ClientError

from src.common.config import ObjectStoreSettings, get_settings
from src.common.logging import get_logger

if TYPE_CHECKING:  # pragma: no cover - typing only
    import pandas as pd

LOGGER = get_logger(__name__)


@dataclass(frozen=True)
class ObjectInfo:
    """One object in the store."""

    key: str
    size_bytes: int
    last_modified: datetime

    @property
    def is_marker(self) -> bool:
        """True for the zero-byte ``.keep`` markers minio_init.sh writes.

        They exist to make the prefix visible in the console and must never
        be counted as data: a dataset holding only markers has zero rows, and
        reporting it as one object would hide a missing load.
        """
        return self.key.endswith("/.keep") or self.size_bytes == 0


@lru_cache(maxsize=1)
def _client(settings: ObjectStoreSettings | None = None) -> Any:
    store = settings or get_settings().object_store
    return boto3.client(
        "s3",
        endpoint_url=store.endpoint,
        aws_access_key_id=store.access_key,
        aws_secret_access_key=store.secret_key,
        region_name=store.region,
        # MinIO requires path-style addressing: a virtual-host style request
        # would resolve the bucket as a subdomain, which does not exist here.
        config=BotoConfig(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
            retries={"max_attempts": 3, "mode": "standard"},
        ),
    )


def reset_client_cache() -> None:
    """Drop the cached client. Used by tests that repoint the endpoint."""
    _client.cache_clear()


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------


def partition_prefix(dataset: str, partition_date: str) -> str:
    """``daily_sales`` + ``2024-01-15`` -> ``daily_sales/dt=2024-01-15``."""
    return f"{dataset}/dt={partition_date}"


def object_key(dataset: str, partition_date: str, filename: str = "part-0.parquet") -> str:
    return f"{partition_prefix(dataset, partition_date)}/{filename}"


def s3_uri(key: str) -> str:
    return f"s3://{get_settings().object_store.bucket}/{key}"


# ---------------------------------------------------------------------------
# Reads
# ---------------------------------------------------------------------------


def list_objects(prefix: str, *, include_markers: bool = False) -> list[ObjectInfo]:
    """List objects under a prefix, paginating fully.

    A single ``list_objects_v2`` call is capped at 1000 keys. Not paginating
    is a classic way to under-count a dataset and report a false volume drop.
    """
    client = _client()
    bucket = get_settings().object_store.bucket
    found: list[ObjectInfo] = []

    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for entry in page.get("Contents", []):
            info = ObjectInfo(
                key=entry["Key"],
                size_bytes=int(entry["Size"]),
                last_modified=entry["LastModified"].astimezone(UTC),
            )
            if info.is_marker and not include_markers:
                continue
            found.append(info)
    return found


def object_exists(key: str) -> bool:
    try:
        _client().head_object(Bucket=get_settings().object_store.bucket, Key=key)
        return True
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") in {"404", "NoSuchKey", "NotFound"}:
            return False
        raise


def object_last_modified(prefix: str) -> datetime | None:
    """Newest object mtime under a prefix, or None when there is nothing.

    None genuinely means "no data here", which the freshness detector reports
    as ``unknown`` rather than as infinitely stale.
    """
    objects = list_objects(prefix)
    if not objects:
        return None
    return max(info.last_modified for info in objects)


def read_bytes(key: str) -> bytes:
    response = _client().get_object(Bucket=get_settings().object_store.bucket, Key=key)
    return response["Body"].read()


def read_parquet(key: str) -> pd.DataFrame:
    """Read one Parquet object into a DataFrame."""
    import pandas as pd

    return pd.read_parquet(io.BytesIO(read_bytes(key)))


def read_partition(dataset: str, partition_date: str) -> pd.DataFrame:
    """Read every Parquet object in one partition as a single DataFrame.

    Returns an empty DataFrame when the partition holds no data objects, so
    callers can distinguish "zero rows" from an exception.
    """
    import pandas as pd

    prefix = partition_prefix(dataset, partition_date)
    keys = [info.key for info in list_objects(prefix) if info.key.endswith(".parquet")]
    if not keys:
        LOGGER.warning("no parquet objects under %s", s3_uri(prefix))
        return pd.DataFrame()
    frames = [read_parquet(key) for key in sorted(keys)]
    return pd.concat(frames, ignore_index=True)


def iter_partitions(dataset: str) -> Iterator[str]:
    """Yield the partition dates present for a dataset, oldest first."""
    seen: set[str] = set()
    for info in list_objects(f"{dataset}/", include_markers=True):
        for part in info.key.split("/"):
            if part.startswith("dt="):
                seen.add(part[3:])
    yield from sorted(seen)


# ---------------------------------------------------------------------------
# Writes
# ---------------------------------------------------------------------------


def write_bytes(key: str, payload: bytes, *, content_type: str = "application/octet-stream") -> str:
    _client().put_object(
        Bucket=get_settings().object_store.bucket,
        Key=key,
        Body=payload,
        ContentType=content_type,
    )
    LOGGER.info("wrote %s (%d bytes)", s3_uri(key), len(payload))
    return key


def write_parquet(key: str, frame: pd.DataFrame, *, compression: str = "snappy") -> str:
    """Serialise a DataFrame to Parquet and store it.

    Written through an in-memory buffer rather than a temporary file so the
    container needs no writable scratch space and a partial object can never
    be left behind by a crash mid-write.
    """
    buffer = io.BytesIO()
    frame.to_parquet(buffer, engine="pyarrow", compression=compression, index=False)
    return write_bytes(key, buffer.getvalue(), content_type="application/vnd.apache.parquet")


def delete_prefix(prefix: str) -> int:
    """Delete every object under a prefix. Returns how many were removed.

    Used by the fault injector's volume scenarios and by the test fixtures.
    Guarded against an empty prefix, which would target the whole bucket.
    """
    if not prefix or prefix in {"/", "*"}:
        raise ValueError("refusing to delete with an empty or wildcard prefix")

    client = _client()
    bucket = get_settings().object_store.bucket
    keys = [{"Key": info.key} for info in list_objects(prefix, include_markers=True)]
    if not keys:
        return 0
    for batch_start in range(0, len(keys), 1000):
        batch = keys[batch_start : batch_start + 1000]
        client.delete_objects(Bucket=bucket, Delete={"Objects": batch})
    LOGGER.info("deleted %d objects under %s", len(keys), s3_uri(prefix))
    return len(keys)


def ping() -> bool:
    """Return True when the bucket is reachable, without raising."""
    try:
        _client().head_bucket(Bucket=get_settings().object_store.bucket)
        return True
    except Exception as exc:  # noqa: BLE001 - deliberately broad: this is a probe
        LOGGER.warning("object store ping failed: %s", exc)
        return False
