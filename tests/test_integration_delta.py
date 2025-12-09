import drainage
from types import SimpleNamespace
import asyncio


class FakeADLSClient:
    def __init__(self, path: str, client_id=None, objects=None):
        # path parsing not required for tests; keep minimal attributes used by analyzer
        self._filesystem = "fs"
        self._account = "account"
        self._prefix = "test-table"
        # prefer explicit objects, fall back to class-level _global_objects set by test
        self._objects = (
            objects
            if objects is not None
            else getattr(FakeADLSClient, "_global_objects", [])
        )

    def get_bucket(self):
        return self._filesystem

    def get_prefix(self):
        return self._prefix

    async def list_objects(self, prefix=None):
        return self._objects

    async def get_object(self, key: str):
        # return a simple JSON add record used by the analyzer parsing
        return b'{"add": {"path": "data/part-00000-0.parquet"}}'


def make_obj(key: str, size: int = 1024):
    return SimpleNamespace(key=key, size=size, last_modified=None)


def test_integration_large_metadata_triggers_recommendation(monkeypatch):
    # Create fake objects: data files + many metadata JSON + a few checkpoint parquet
    data_files = [
        make_obj(f"data/part-{i:05d}.parquet", size=5 * 1024 * 1024) for i in range(5)
    ]
    metadata_json = [
        make_obj(f"_delta_log/{i:05d}.json", size=1024) for i in range(120)
    ]
    checkpoints = [
        make_obj(f"_delta_log/{i:05d}.checkpoint.parquet", size=2048) for i in range(3)
    ]
    all_objs = data_files + metadata_json + checkpoints

    # Patch the public ADLSClient used by the analyze API and provide objects via a class-level field
    FakeADLSClient._global_objects = all_objs
    monkeypatch.setattr(drainage, "ADLSClient", FakeADLSClient)

    # Run analysis via public API (defaults should recommend cleanup)
    report = drainage.analyze_delta_lake(
        "abfss://fs@account.dfs.core.windows.net/test-table/"
    )

    assert report.metrics.metadata_health is not None
    assert report.metrics.metadata_health.metadata_file_count == len(
        metadata_json
    ) + len(checkpoints)
    assert any(
        "metadata" in r.lower() or "large" in r for r in report.metrics.recommendations
    )


def test_integration_threshold_override_suppresses_recommendation(monkeypatch):
    data_files = [
        make_obj(f"data/part-{i:05d}.parquet", size=5 * 1024 * 1024) for i in range(5)
    ]
    metadata_json = [make_obj(f"_delta_log/{i:05d}.json", size=1024) for i in range(60)]
    checkpoints = [
        make_obj(f"_delta_log/{i:05d}.checkpoint.parquet", size=2048) for i in range(2)
    ]
    all_objs = data_files + metadata_json + checkpoints

    FakeADLSClient._global_objects = all_objs
    monkeypatch.setattr(drainage, "ADLSClient", FakeADLSClient)

    # Provide higher thresholds so defaults won't trigger
    report = drainage.analyze_delta_lake(
        "abfss://fs@account.dfs.core.windows.net/test-table/",
        metadata_file_count_threshold=200,
        metadata_total_size_threshold=100 * 1024 * 1024,
    )

    assert report.metrics.metadata_health is not None
    assert report.metrics.metadata_health.metadata_file_count == len(
        metadata_json
    ) + len(checkpoints)
    assert not any(
        "metadata" in r.lower() or "large" in r for r in report.metrics.recommendations
    )
