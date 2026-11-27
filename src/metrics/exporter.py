"""Prometheus exporter process.

    python -m src.metrics.exporter

Serves /metrics on DQ_EXPORTER_PORT. The default registry is replaced with a
clean one holding only this platform's collector plus the standard process
collectors, so the output is not padded with the client library's own
defaults.
"""

from __future__ import annotations

import contextlib
import signal
import sys
import time
from types import FrameType

from prometheus_client import REGISTRY, start_http_server
from prometheus_client.gc_collector import GCCollector
from prometheus_client.platform_collector import PlatformCollector
from prometheus_client.process_collector import ProcessCollector

from src.common.config import get_settings
from src.common.db import ping
from src.common.logging import get_logger
from src.metrics.collectors import DataQualityCollector

LOGGER = get_logger(__name__)

_RUNNING = True


def _handle_signal(signum: int, _frame: FrameType | None) -> None:
    global _RUNNING
    LOGGER.info("received signal %s, shutting down", signum)
    _RUNNING = False


def build_registry():
    """Register the platform collector on the default registry.

    The library's own default collectors are unregistered first: process and
    platform metrics are useful, GC metrics are noise at this scale, and
    leaving the defaults in place makes the scrape output harder to read when
    someone is looking for a dq_ series.
    """
    for collector in list(REGISTRY._collector_to_names):  # noqa: SLF001
        if isinstance(collector, GCCollector | PlatformCollector | ProcessCollector):
            # Already gone is the desired state, not an error: the list is
            # snapshotted before the loop, so a collector can disappear from
            # the registry before we reach it.
            with contextlib.suppress(KeyError):
                REGISTRY.unregister(collector)

    ProcessCollector(registry=REGISTRY)
    REGISTRY.register(DataQualityCollector())
    return REGISTRY


def main() -> int:
    settings = get_settings()
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    # Fail loudly at startup if the database is unreachable, rather than
    # serving an endpoint that answers every scrape with a collection error.
    if not ping(settings.observability):
        LOGGER.error(
            "cannot reach %s; the exporter would serve nothing useful",
            settings.observability.redacted(),
        )
        return 1

    build_registry()
    start_http_server(settings.exporter_port)
    LOGGER.info(
        "exporter listening on :%s/metrics (reading %s)",
        settings.exporter_port,
        settings.observability.redacted(),
    )

    while _RUNNING:
        time.sleep(1)

    LOGGER.info("exporter stopped")
    return 0


if __name__ == "__main__":
    sys.exit(main())
