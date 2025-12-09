"""ADLS (Azure Data Lake Storage) client wrapper supporting UAMI.

This module provides a minimal client API compatible with the
existing code's expectations: `list_objects(prefix)`, `get_object(key)`,
`get_bucket()` and `get_prefix()`.

Authentication supported:
- User assigned managed identity: pass `client_id` to `ADLSClient`
- DefaultAzureCredential fallback for other environments

"""

from typing import List, Optional
from dataclasses import dataclass
from azure.identity import DefaultAzureCredential, ManagedIdentityCredential
from azure.storage.filedatalake import DataLakeServiceClient
from urllib.parse import urlparse


@dataclass
class ObjectInfo:
    key: str
    size: int
    last_modified: Optional[str]


class ADLSClient:
    def __init__(self, path: str, client_id: Optional[str] = None):
        """Create an ADLS client.

        path: ADLS URL such as:
          - abfss://<filesystem>@<account>.dfs.core.windows.net/<prefix>
          - https://<account>.dfs.core.windows.net/<filesystem>/<prefix>

        client_id: optional user-assigned managed identity client id
        """
        self.raw_path = path
        self._account = None
        self._filesystem = None
        self._prefix = ""

        parsed = urlparse(path)
        # handle abfss scheme
        if parsed.scheme == "abfss":
            # netloc is like <filesystem>@<account>.dfs.core.windows.net
            netloc = parsed.netloc
            if "@" in netloc:
                fs, host = netloc.split("@", 1)
                self._filesystem = fs
                # host like <account>.dfs.core.windows.net
                self._account = host.split(".")[0]
            self._prefix = parsed.path.lstrip("/")
        else:
            # fallback: parse https://<account>.dfs.core.windows.net/<filesystem>/<prefix>
            if parsed.scheme.startswith("http") and parsed.netloc:
                host_parts = parsed.netloc.split(".")
                self._account = host_parts[0]
                parts = parsed.path.lstrip("/").split("/", 1)
                self._filesystem = parts[0] if parts else None
                self._prefix = parts[1] if len(parts) > 1 else ""

        # Choose credential
        if client_id:
            credential = ManagedIdentityCredential(client_id=client_id)
        else:
            credential = DefaultAzureCredential()

        if not self._account or not self._filesystem:
            raise ValueError(f"Failed to parse ADLS path: {path}")

        account_url = f"https://{self._account}.dfs.core.windows.net"
        self._service = DataLakeServiceClient(
            account_url=account_url, credential=credential
        )
        self._filesystem_client = self._service.get_file_system_client(self._filesystem)

    def get_bucket(self) -> str:
        return self._filesystem

    def get_prefix(self) -> str:
        return self._prefix

    async def list_objects(self, prefix: Optional[str] = None) -> List[ObjectInfo]:
        """List objects under the given prefix. Returns list of ObjectInfo.

        Note: the ADLS SDK is synchronous; we keep an async signature to
        match the existing code expectations. Callers may `await` this, but
        under the hood it runs sync calls — this is a pragmatic interim
        choice. If you prefer fully async behavior, we can run the sync
        client in a threadpool.
        """
        effective_prefix = prefix or self._prefix
        results: List[ObjectInfo] = []

        path_iter = self._filesystem_client.get_paths(
            path=effective_prefix, recursive=True
        )
        for p in path_iter:
            # skip directories
            if p.is_directory:
                continue
            key = p.name
            size = p.content_length or 0
            last_mod = p.last_modified.isoformat() if p.last_modified else None
            results.append(ObjectInfo(key=key, size=size, last_modified=last_mod))
        return results

    async def get_object(self, key: str) -> bytes:
        file_client = self._filesystem_client.get_file_client(key)
        download = file_client.download_file()
        data = download.readall()
        return data
