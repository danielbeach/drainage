"""Drainage Python package.

Provides a concise, Python-first API for analyzing Delta Lake tables
stored on Azure Data Lake Storage (ADLS). Exposes the ADLS client
and synchronous helpers for running the Delta Lake analyzer.
"""

from .adls_client import ADLSClient
from .delta_lake import DeltaLakeAnalyzer
from .types import HealthReport
import asyncio
import threading
import os

__all__ = ["ADLSClient", "analyze_table", "analyze_delta_lake", "analyze_delta_lake_async"]


async def _analyze_delta_async(
    path: str,
    client_id: str = None,
    metadata_file_count_threshold: int | None = None,
    metadata_total_size_threshold: int | None = None,
) -> HealthReport:
    client_id = client_id or os.getenv("UAMI_DEFAULT_CLIENT_ID")
    client = ADLSClient(path, client_id)
    # If thresholds are None, DeltaLakeAnalyzer will use its internal defaults
    analyzer = DeltaLakeAnalyzer(
        client,
        metadata_file_count_threshold=metadata_file_count_threshold,
        metadata_total_size_threshold=metadata_total_size_threshold,
    )
    return await analyzer.analyze()


def _run_blocking(coro):
    """Run a coroutine from sync code, even if a loop is already running (blocks caller)."""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)

    result_container = {}
    exception_container = {}

    def runner():
        try:
            result_container["value"] = asyncio.run(coro)
        except Exception as exc:  # pragma: no cover - bubble to caller
            exception_container["error"] = exc

    t = threading.Thread(target=runner, daemon=True)
    t.start()
    t.join()
    if "error" in exception_container:
        raise exception_container["error"]
    return result_container.get("value")


async def analyze_delta_lake_async(
    path: str,
    client_id: str = None,
    metadata_file_count_threshold: int | None = None,
    metadata_total_size_threshold: int | None = None,
) -> HealthReport:
    """Async-friendly analyzer entrypoint for integration in async servers."""
    return await _analyze_delta_async(
        path,
        client_id,
        metadata_file_count_threshold=metadata_file_count_threshold,
        metadata_total_size_threshold=metadata_total_size_threshold,
    )


def analyze_delta_lake(
    path: str,
    client_id: str = None,
    metadata_file_count_threshold: int | None = None,
    metadata_total_size_threshold: int | None = None,
) -> HealthReport:
    """Analyze a Delta Lake table located on ADLS.

    `path` should be an ADLS URL (e.g. `abfss://<filesystem>@<account>.dfs.core.windows.net/<prefix>`)
    `client_id` is optional user-assigned managed identity client id (UAMI)
    Safe for callers that already have a running asyncio loop (runs the analyzer in a helper thread).
    """
    return _run_blocking(
        _analyze_delta_async(
            path,
            client_id,
            metadata_file_count_threshold=metadata_file_count_threshold,
            metadata_total_size_threshold=metadata_total_size_threshold,
        )
    )


def analyze_table(
    path: str, table_type: str = None, client_id: str = None, **kwargs
) -> HealthReport:
    if table_type and table_type.lower() in ("delta", "delta_lake"):
        return analyze_delta_lake(path, client_id, **kwargs)
    if table_type and table_type.lower() in ("iceberg", "apache_iceberg"):
        raise NotImplementedError("Iceberg analysis is not supported")
    return analyze_delta_lake(path, client_id, **kwargs)


def print_health_report(report: HealthReport) -> None:
    """Print a human-friendly health report for a `HealthReport` dataclass."""
    print("\n" + "=" * 60)
    print(f"Table Health Report: {report.table_path}")
    print(f"Type: {report.table_type}")
    print(f"Analysis Time: {report.analysis_timestamp}")
    print("" + "=" * 60 + "\n")

    print("📊 Key Metrics:")
    print("─" * 60)
    m = report.metrics
    print(f"  Total Files:         {m.total_files}")
    if m.total_size_bytes >= 1024**3:
        print(f"  Total Size:          {m.total_size_bytes / (1024**3):.2f} GB")
    else:
        print(f"  Total Size:          {m.total_size_bytes / (1024**2):.2f} MB")
    print(f"  Average File Size:   {m.avg_file_size_bytes / (1024**2):.2f} MB")
    print(f"  Partition Count:     {m.partition_count}")

    if getattr(m, "unreferenced_files", None):
        print("\n⚠️  Unreferenced Files:")
        print("─" * 60)
        print(f"  Count:  {len(m.unreferenced_files)}")


__all__.append("print_health_report")
