"""Delta Lake analyzer ported to Python.

This implementation mirrors the core logic needed to:
- list objects under a prefix
- separate data files and metadata (Delta _delta_log JSON files)
- parse add actions from transaction JSON to find referenced files
- compute basic metrics and unreferenced files

It uses `delta_skelter.ADLSClient` for storage access and the dataclasses
in `delta_skelter.types`.
"""

from typing import List
import json
from .adls_client import ADLSClient
from .types import FileInfo, HealthReport, HealthMetrics


class DeltaLakeAnalyzer:
    def __init__(
        self,
        adls_client: ADLSClient,
        metadata_file_count_threshold: int | None = 100,
        metadata_total_size_threshold: int | None = 50 * 1024 * 1024,
    ):
        self.client = adls_client
        # thresholds for recommending metadata cleanups; accept None and coerce to sensible defaults
        self.metadata_file_count_threshold = (
            metadata_file_count_threshold
            if metadata_file_count_threshold is not None
            else 100
        )
        self.metadata_total_size_threshold = (
            metadata_total_size_threshold
            if metadata_total_size_threshold is not None
            else 50 * 1024 * 1024
        )

    async def analyze(self) -> HealthReport:
        account = getattr(self.client, "get_account", None)
        account_name = (
            account() if callable(account) else getattr(self.client, "_account", "")
        )
        table_path = f"abfss://{self.client.get_bucket()}@{account_name}.dfs.core.windows.net/{self.client.get_prefix()}"
        report = HealthReport.new(table_path, "delta")

        # list all objects
        all_objects = await self.client.list_objects(self.client.get_prefix())

        # separate data and metadata. Data files are parquet files outside the _delta_log
        data_files = [
            o
            for o in all_objects
            if o.key.endswith(".parquet") and "_delta_log/" not in o.key
        ]
        # metadata files are any files under the _delta_log directory (JSON transactions and checkpoint parquet)
        metadata_files = [o for o in all_objects if "_delta_log/" in o.key]

        # parse metadata files (transaction log) to find referenced files and table properties
        referenced = set()
        table_properties: dict[str, str] = {}
        # parse only JSON transaction files to discover referenced data files
        parse_errors = 0
        for m in [mf for mf in metadata_files if mf.key.endswith(".json")]:
            try:
                content = await self.client.get_object(m.key)
                text = content.decode("utf-8", errors="replace")
            except Exception:
                parse_errors += 1
                continue

            # NDJSON: one JSON object per line or a single JSON array/object
            for line in text.splitlines():
                line = line.strip()
                if not line:
                    continue
                parsed_full_content = False
                try:
                    j = json.loads(line)
                except Exception:
                    # try full content as JSON fallback
                    try:
                        j = json.loads(text)
                        parsed_full_content = True
                        # if successful, process and break
                    except Exception:
                        parse_errors += 1
                        break

                # look for add actions
                if isinstance(j, dict):
                    if "add" in j and isinstance(j["add"], dict):
                        # older format where add is an object
                        path = j["add"].get("path")
                        if path:
                            referenced.add(path)
                    elif "add" in j and isinstance(j["add"], list):
                        for item in j["add"]:
                            if isinstance(item, dict) and "path" in item:
                                referenced.add(item["path"])
                    if "metaData" in j and isinstance(j["metaData"], dict):
                        cfg = j["metaData"].get("configuration")
                        if isinstance(cfg, dict):
                            table_properties.update(
                                {str(k): str(v) for k, v in cfg.items()}
                            )
                # once full-content parse succeeds, stop iterating lines to avoid duplicate work
                if parsed_full_content:
                    break

        # --- Metadata health: count and sizes of _delta_log files ---
        metadata_count = len(metadata_files)
        metadata_total = sum(m.size for m in metadata_files)
        avg_metadata_size = metadata_total / metadata_count if metadata_count > 0 else 0
        # Count checkpoint/parquet files inside the _delta_log (checkpoints are parquet)
        manifest_file_count = sum(1 for m in metadata_files if m.key.endswith(".parquet"))
        checkpoint_files = [m for m in metadata_files if m.key.endswith(".parquet")]
        checkpoint_count = len(checkpoint_files)
        checkpoint_total_size = sum(m.size for m in checkpoint_files)
        checkpoint_times = [getattr(m, "last_modified", None) for m in checkpoint_files if getattr(m, "last_modified", None)]
        oldest_checkpoint_ts = min(checkpoint_times) if checkpoint_times else None
        latest_checkpoint_ts = max(checkpoint_times) if checkpoint_times else None

        # Compute metrics
        metrics = HealthMetrics()
        metrics.total_files = len(data_files)
        metrics.total_size_bytes = sum(o.size for o in data_files)

        # populate metadata health structure
        try:
            # MetadataHealth is part of HealthMetrics; ensure it's populated
            metrics.metadata_health.metadata_file_count = metadata_count
            metrics.metadata_health.metadata_total_size_bytes = metadata_total
            metrics.metadata_health.avg_metadata_file_size_bytes = avg_metadata_size
            metrics.metadata_health.manifest_file_count = manifest_file_count
            metrics.metadata_health.checkpoint_count = checkpoint_count
            metrics.metadata_health.checkpoint_total_size_bytes = checkpoint_total_size
            metrics.metadata_health.oldest_checkpoint_timestamp = oldest_checkpoint_ts
            metrics.metadata_health.latest_checkpoint_timestamp = latest_checkpoint_ts
        except Exception:
            # If metadata_health is None or missing, create and attach a simple object
            mh = type("MH", (), {})()
            mh.metadata_file_count = metadata_count
            mh.metadata_total_size_bytes = metadata_total
            mh.avg_metadata_file_size_bytes = avg_metadata_size
            mh.manifest_file_count = manifest_file_count
            mh.checkpoint_count = checkpoint_count
            mh.checkpoint_total_size_bytes = checkpoint_total_size
            mh.oldest_checkpoint_timestamp = oldest_checkpoint_ts
            mh.latest_checkpoint_timestamp = latest_checkpoint_ts
            metrics.metadata_health = mh

        # Simple heuristic: if many metadata files or large metadata size, recommend metadata cleanup
        if (
            metadata_count >= self.metadata_file_count_threshold
            or metadata_total >= self.metadata_total_size_threshold
        ):
            metrics.recommendations.append(
                "Large Delta log detected — many transaction/checkpoint files; consider optimizing metadata (compact checkpoints, run VACUUM/OPTIMIZE)."
            )

        if parse_errors > 0:
            metrics.recommendations.append(
                "One or more Delta log entries could not be parsed; check _delta_log for corruption or unsupported format."
            )

        # find unreferenced files
        for f in data_files:
            file_path = f"{self.client.get_prefix()}/{f.key}"
            if file_path not in referenced and f.key not in referenced:
                metrics.unreferenced_files.append(
                    FileInfo(
                        path=file_path,
                        size_bytes=f.size,
                        last_modified=f.last_modified,
                        is_referenced=False,
                    )
                )

        metrics.unreferenced_size_bytes = sum(
            fi.size_bytes for fi in metrics.unreferenced_files
        )

        metrics.table_properties.update(table_properties)

        # average file size
        if metrics.total_files > 0:
            metrics.avg_file_size_bytes = metrics.total_size_bytes / metrics.total_files

        # basic file size distribution
        for f in data_files:
            s = f.size
            if s < 16 * 1024 * 1024:
                metrics.file_size_distribution.small_files += 1
            elif s < 128 * 1024 * 1024:
                metrics.file_size_distribution.medium_files += 1
            elif s < 1024 * 1024 * 1024:
                metrics.file_size_distribution.large_files += 1
            else:
                metrics.file_size_distribution.very_large_files += 1

        # basic health score heuristic
        total = metrics.total_files or 1
        unref_percent = (
            metrics.unreferenced_size_bytes / max(1, metrics.total_size_bytes)
            if metrics.total_size_bytes > 0
            else 0
        )
        metrics.health_score = max(0.0, 1.0 - unref_percent)

        report.metrics = metrics
        report.health_score = metrics.health_score
        return report
