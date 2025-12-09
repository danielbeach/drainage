import asyncio
from types import SimpleNamespace

from delta_skelter.delta_lake import DeltaLakeAnalyzer


class FakeADLSClient:
    def __init__(self, bucket: str, account: str, prefix: str, objects):
        self._filesystem = bucket
        self._account = account
        self._prefix = prefix
        self._objects = objects

    def get_bucket(self):
        return self._filesystem

    def get_prefix(self):
        return self._prefix

    async def list_objects(self, prefix=None):
        return self._objects

    async def get_object(self, key: str):
        # Return a small JSON payload for metadata files if requested
        return b'{"add": {"path": "data/part-00000-0.parquet"}}'


def make_obj(key: str, size: int = 1024):
    return SimpleNamespace(key=key, size=size, last_modified=None)


def test_bloated_delta_log_detection():
    # create many metadata files inside _delta_log (JSON transaction files)
    metadata_files = [
        make_obj(f"_delta_log/{i:05d}.json", size=1024) for i in range(150)
    ]
    # add some checkpoint parquet files as metadata
    metadata_files += [
        make_obj(f"_delta_log/{i:05d}.checkpoint.parquet", size=2048) for i in range(5)
    ]

    # create some data files
    data_files = [
        make_obj(f"data/part-{i:05d}.parquet", size=10 * 1024 * 1024) for i in range(10)
    ]

    all_objects = data_files + metadata_files

    # Fake client
    client = FakeADLSClient(
        bucket="fs", account="account", prefix="test-table", objects=all_objects
    )

    analyzer = DeltaLakeAnalyzer(client)

    report = asyncio.run(analyzer.analyze())

    # Ensure metadata health fields were populated
    mh = report.metrics.metadata_health
    assert mh is not None
    assert mh.metadata_file_count == len(metadata_files)
    assert mh.metadata_total_size_bytes == sum(m.size for m in metadata_files)
    # manifest/checkpoint count should include the parquet checkpoint files
    assert mh.manifest_file_count >= 5

    # If metadata file count is large, analyzer should recommend cleanup
    recs = report.metrics.recommendations
    # The analyzer appends recommendations when thresholds exceeded (>=100 files)
    assert any("Large" in r or "metadata" in r.lower() for r in recs)
