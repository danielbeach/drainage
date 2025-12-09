import drainage
from types import SimpleNamespace


def patch_clients(monkeypatch, client_cls):
    """Ensure both public and internal analyzer use the patched ADLS client."""
    import drainage.delta_lake as dl
    import drainage.adls_client as adls

    monkeypatch.setattr(drainage, "ADLSClient", client_cls)
    monkeypatch.setattr(dl, "ADLSClient", client_cls)
    monkeypatch.setattr(adls, "ADLSClient", client_cls)


class FakeADLSClientSimple:
    def __init__(self, path: str, client_id=None, objects=None):
        self._filesystem = "fs"
        self._account = "account"
        self._prefix = "test"
        # allow per-instance objects or global class-provided list
        self._objects = (
            objects
            if objects is not None
            else getattr(FakeADLSClientSimple, "_global_objects", [])
        )

    def get_bucket(self):
        return self._filesystem

    def get_prefix(self):
        return self._prefix

    async def list_objects(self, prefix=None):
        return self._objects

    async def get_object(self, key: str):
        # return a transaction that references a single file if needed
        return b'{"add": {"path": "data/part-00000-0.parquet"}}'


def make_obj(key: str, size: int = 1024):
    return SimpleNamespace(key=key, size=size, last_modified=None)


def test_analyze_table_forwards_thresholds(monkeypatch):
    # small dataset with many metadata files, but we'll set a tiny threshold
    data_files = [make_obj("data/part-00000-0.parquet", size=1024)]
    metadata = [make_obj(f"_delta_log/{i:05d}.json", size=512) for i in range(10)]
    FakeADLSClientSimple._global_objects = data_files + metadata
    patch_clients(monkeypatch, FakeADLSClientSimple)
    # sanity: patched client returns our objects
    import asyncio

    assert drainage.ADLSClient is FakeADLSClientSimple

    objs = asyncio.run(drainage.ADLSClient("unused").list_objects(None))
    assert len(objs) == len(data_files) + len(metadata)

    # analyze_table should forward threshold kwargs to analyze_delta_lake
    report = drainage.analyze_table(
        "abfss://fs@account.dfs.core.windows.net/test/",
        table_type="delta",
        metadata_file_count_threshold=5,
    )

    assert report.metrics.metadata_health is not None
    assert report.metrics.metadata_health.metadata_file_count == len(metadata)
    assert any(
        "large" in r.lower() or "metadata" in r.lower()
        for r in report.metrics.recommendations
    )


def test_unreferenced_files_detection(monkeypatch):
    # three data files, but metadata only references one
    data_files = [make_obj(f"data/part-{i:05d}.parquet", size=1024) for i in range(3)]
    # metadata JSON references only the first file
    metadata_json = [make_obj("_delta_log/00000.json", size=100)]
    FakeADLSClientSimple._global_objects = data_files + metadata_json
    patch_clients(monkeypatch, FakeADLSClientSimple)
    import asyncio

    objs = asyncio.run(drainage.ADLSClient("unused").list_objects(None))
    assert len(objs) == len(data_files) + len(metadata_json)

    report = drainage.analyze_delta_lake(
        "abfss://fs@account.dfs.core.windows.net/test/"
    )

    assert report.metrics.total_files == 3
    # two files should be unreferenced
    assert len(report.metrics.unreferenced_files) >= 2


def test_manifest_file_count_counts_parquet_checkpoint(monkeypatch):
    data_files = [make_obj("data/part-00000-0.parquet", size=1024)]
    metadata_files = [
        make_obj("_delta_log/00000.json", size=100),
        make_obj("_delta_log/00001.checkpoint.parquet", size=2048),
        make_obj("_delta_log/00002.parquet", size=2048),
    ]
    FakeADLSClientSimple._global_objects = data_files + metadata_files
    patch_clients(monkeypatch, FakeADLSClientSimple)
    import asyncio

    objs = asyncio.run(drainage.ADLSClient("unused").list_objects(None))
    assert len(objs) == len(data_files) + len(metadata_files)

    report = drainage.analyze_delta_lake(
        "abfss://fs@account.dfs.core.windows.net/test/"
    )

    mh = report.metrics.metadata_health
    assert mh is not None
    # manifest_file_count should count parquet/checkpoint files under _delta_log
    assert mh.manifest_file_count >= 2


class FakeADLSClientCorrupt:
    def __init__(self, path: str, client_id=None, objects=None, content_map=None):
        self._filesystem = "fs"
        self._account = "account"
        self._prefix = "test"
        self._objects = (
            objects
            if objects is not None
            else getattr(FakeADLSClientSimple, "_global_objects", [])
        )
        # map of key -> bytes or Exception to raise
        self._content_map = content_map or {}

    def get_bucket(self):
        return self._filesystem

    def get_prefix(self):
        return self._prefix

    async def list_objects(self, prefix=None):
        return self._objects

    async def get_object(self, key: str):
        v = self._content_map.get(key)
        if isinstance(v, Exception):
            raise v
        if v is None:
            # default simple transaction
            return b'{"add": {"path": "data/part-00000-0.parquet"}}'
        return v


def test_corrupted_metadata_json_does_not_crash(monkeypatch):
    # truncated JSON should be handled gracefully and not crash analyzer
    data_files = [make_obj("data/part-00000-0.parquet", size=1024)]
    metadata = [make_obj("_delta_log/00000.json", size=10)]
    FakeADLSClientCorrupt._global_objects = data_files + metadata
    # truncated content (missing closing braces)
    content_map = {"_delta_log/00000.json": b'{"add": {"path": "data/part-00000-0.parquet"'}
    patch_clients(monkeypatch, lambda *a, **k: FakeADLSClientCorrupt(*a, **k, content_map=content_map))

    report = drainage.analyze_delta_lake("abfss://fs@account.dfs.core.windows.net/test/")
    # since parser couldn't successfully extract references, file should be considered unreferenced
    assert len(report.metrics.unreferenced_files) >= 1


def test_non_utf8_bytes_in_metadata_are_handled(monkeypatch):
    data_files = [make_obj("data/part-00000-0.parquet", size=1024)]
    metadata = [make_obj("_delta_log/00000.json", size=10)]
    FakeADLSClientCorrupt._global_objects = data_files + metadata
    # prepend an invalid byte; decoder uses errors='replace' so this simulates malformed bytes
    content_map = {"_delta_log/00000.json": b"\xff{" + b'"add": {"path": "data/part-00000-0.parquet"}}'}
    patch_clients(monkeypatch, lambda *a, **k: FakeADLSClientCorrupt(*a, **k, content_map=content_map))

    report = drainage.analyze_delta_lake("abfss://fs@account.dfs.core.windows.net/test/")
    assert len(report.metrics.unreferenced_files) >= 1


def test_get_object_exception_skips_metadata(monkeypatch):
    data_files = [make_obj("data/part-00000-0.parquet", size=1024)]
    metadata = [make_obj("_delta_log/00000.json", size=10)]
    FakeADLSClientCorrupt._global_objects = data_files + metadata
    # simulate storage error while fetching metadata
    content_map = {"_delta_log/00000.json": RuntimeError("storage read error")}
    patch_clients(monkeypatch, lambda *a, **k: FakeADLSClientCorrupt(*a, **k, content_map=content_map))

    report = drainage.analyze_delta_lake("abfss://fs@account.dfs.core.windows.net/test/")
    # metadata fetch failed, so references are unknown -> file considered unreferenced
    assert len(report.metrics.unreferenced_files) >= 1


def test_ndjson_multiline_parsing(monkeypatch):
    # NDJSON with two add actions should reference both files
    data_files = [
        make_obj("data/part-00000-0.parquet", size=1024),
        make_obj("data/part-00001-0.parquet", size=1024),
    ]
    metadata = [make_obj("_delta_log/00000.json", size=200)]
    FakeADLSClientCorrupt._global_objects = data_files + metadata
    ndjson = b'{"add": {"path": "data/part-00000-0.parquet"}}\n{"add": {"path": "data/part-00001-0.parquet"}}'
    content_map = {"_delta_log/00000.json": ndjson}
    patch_clients(monkeypatch, lambda *a, **k: FakeADLSClientCorrupt(*a, **k, content_map=content_map))

    report = drainage.analyze_delta_lake("abfss://fs@account.dfs.core.windows.net/test/")
    # both files are referenced, so unreferenced list should be empty
    assert len(report.metrics.unreferenced_files) == 0
