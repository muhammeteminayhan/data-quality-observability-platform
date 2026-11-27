"""Great Expectations context and data sources.

Uses the current GX Core 1.x workflow throughout:

    Data Context -> Data Source -> Data Asset -> Batch Definition
                 -> Expectation Suite -> Validation Definition -> Checkpoint

None of the deprecated 0.18 concepts appear anywhere: there is no
``great_expectations.yml`` v2 config, no ``DataConnector``, no
``BatchRequest`` dictionary and no ``context.run_checkpoint(...)`` by name.

Two findings from running this against the real stack, both non-obvious:

1. A ``pandas_s3`` parquet asset lists objects NON-recursively by default, so
   a Hive-style layout (``daily_sales/dt=2024-01-15/part-0.parquet``) is
   invisible and the asset fails at ``test_connection`` with
   "No file in bucket ... found using delimiter". ``s3_recursive_file_discovery
   =True`` is required.

2. ``add_batch_definition_path`` takes a path RELATIVE to ``s3_prefix``.
   Passing the full key raises ``PathNotFoundError`` for an object that is
   plainly there.
"""

from __future__ import annotations

from functools import lru_cache
from typing import Any

import great_expectations as gx
from great_expectations.data_context import AbstractDataContext

from src.common.config import get_settings
from src.common.logging import get_logger

LOGGER = get_logger(__name__)

POSTGRES_DATASOURCE = "commerce_postgres"
LAKE_DATASOURCE = "commerce_lake"


@lru_cache(maxsize=1)
def get_context() -> AbstractDataContext:
    """An ephemeral GX context.

    Ephemeral on purpose. A file-backed context would make GX the owner of
    suite and result history, which this platform deliberately keeps in
    observability_db instead: the suites are derived from the contracts on
    every run, so persisting them would create a second source of truth that
    could drift from ``contracts/``.
    """
    context = gx.get_context(mode="ephemeral")
    LOGGER.info("initialised ephemeral GX context (gx %s)", gx.__version__)
    return context


def reset_context_cache() -> None:
    get_context.cache_clear()


def postgres_datasource(context: AbstractDataContext | None = None) -> Any:
    """The commerce_db SQL data source."""
    ctx = context or get_context()
    settings = get_settings()
    return ctx.data_sources.add_or_update_postgres(
        name=POSTGRES_DATASOURCE,
        connection_string=settings.source.sqlalchemy_url,
    )


def lake_datasource(context: AbstractDataContext | None = None) -> Any:
    """The MinIO (S3 API) data source for the Parquet datasets."""
    ctx = context or get_context()
    store = get_settings().object_store
    return ctx.data_sources.add_or_update_pandas_s3(
        name=LAKE_DATASOURCE,
        bucket=store.bucket,
        boto3_options={
            "endpoint_url": store.endpoint,
            "aws_access_key_id": store.access_key,
            "aws_secret_access_key": store.secret_key,
            "region_name": store.region,
        },
    )


def postgres_batch_definition(dataset_location: str, context: AbstractDataContext | None = None) -> Any:
    """A whole-table batch definition for ``schema.table``."""
    ctx = context or get_context()
    datasource = postgres_datasource(ctx)
    parts = dataset_location.split(".")
    schema_name, table_name = (parts[0], parts[1]) if len(parts) == 2 else (None, parts[0])

    asset_name = dataset_location.replace(".", "_")
    try:
        asset = datasource.get_asset(asset_name)
    except (LookupError, KeyError):
        asset = datasource.add_table_asset(
            name=asset_name, table_name=table_name, schema_name=schema_name
        )

    definition_name = f"{asset_name}__whole_table"
    try:
        return asset.get_batch_definition(definition_name)
    except (LookupError, KeyError):
        return asset.add_batch_definition_whole_table(definition_name)


def parquet_batch_definition(
    dataset_name: str,
    partition_date: str,
    *,
    filename: str = "part-0.parquet",
    context: AbstractDataContext | None = None,
) -> Any:
    """A batch definition for one Parquet partition.

    ``dataset_name`` is the prefix (e.g. ``daily_sales``) and the batch path is
    relative to it.
    """
    ctx = context or get_context()
    datasource = lake_datasource(ctx)

    try:
        asset = datasource.get_asset(dataset_name)
    except (LookupError, KeyError):
        asset = datasource.add_parquet_asset(
            name=dataset_name,
            s3_prefix=f"{dataset_name}/",
            # Required: without it the partitioned layout is not discovered.
            s3_recursive_file_discovery=True,
        )

    relative_path = f"dt={partition_date}/{filename}"
    definition_name = f"{dataset_name}__{partition_date}"
    try:
        return asset.get_batch_definition(definition_name)
    except (LookupError, KeyError):
        return asset.add_batch_definition_path(definition_name, path=relative_path)
