import os
import asyncio
from dataclasses import asdict
from typing import Optional
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, HttpUrl, validator

import drainage

DEFAULT_CLIENT_ID = os.getenv("UAMI_DEFAULT_CLIENT_ID", "UAMI_DEV_NEU_AT42938_AKS_RT")


class AnalyzeRequest(BaseModel):
    path: str = Field(..., description="ADLS Delta Lake path (abfss or https)")
    client_id: Optional[str] = Field(None, description="Override UAMI client id")
    metadata_file_count_threshold: Optional[int] = None
    metadata_total_size_threshold: Optional[int] = None
    demo_mode: bool = Field(False, description="Run against built-in demo data instead of ADLS")

    @validator("path")
    def validate_path(cls, v: str) -> str:
        # allow arbitrary in demo mode; for normal mode require abfss/https
        if not (v.startswith("abfss://") or v.startswith("https://") or v.startswith("demo://")):
            raise ValueError("Path must start with abfss://, https://, or demo://")
        return v


class FakeDemoClient:
    """Lightweight in-memory client for demo mode."""

    def __init__(self):
        self._filesystem = "demo-fs"
        self._account = "demo-account"
        self._prefix = "demo-table"
        # demo objects: referenced + unreferenced data, multiple metadata files, checkpoint, manifest parquet
        self._objects = [
            # data files (two referenced, one unreferenced, one large)
            type("O", (), {"key": "data/part-00000.parquet", "size": 1024 * 10, "last_modified": None}),
            type("O", (), {"key": "data/part-00001.parquet", "size": 1024 * 12, "last_modified": None}),
            type("O", (), {"key": "data/unref-00002.parquet", "size": 1024 * 8, "last_modified": None}),
            type("O", (), {"key": "data/large-00003.parquet", "size": 1024 * 1024 * 64, "last_modified": None}),
            # metadata json (two transaction logs)
            type("O", (), {"key": "_delta_log/00000.json", "size": 400, "last_modified": None}),
            type("O", (), {"key": "_delta_log/00001.json", "size": 450, "last_modified": None}),
            # checkpoint parquet counted as manifest
            type("O", (), {"key": "_delta_log/00002.checkpoint.parquet", "size": 1024 * 5, "last_modified": None}),
            # manifest parquet
            type("O", (), {"key": "_delta_log/00003.parquet", "size": 1024 * 3, "last_modified": None}),
        ]

    def get_bucket(self):
        return self._filesystem

    def get_prefix(self):
        return self._prefix

    def get_account(self):
        return self._account

    async def list_objects(self, prefix=None):
        return self._objects

    async def get_object(self, key: str):
        if key.endswith("00000.json"):
            return b'{"add": {"path": "demo-table/data/part-00000.parquet"}}'
        if key.endswith("00001.json"):
            return b'{"add": {"path": "demo-table/data/part-00001.parquet"}}\n{"add": {"path": "demo-table/data/large-00003.parquet"}}'
        return b""


def _report_to_dict(report: drainage.HealthReport) -> dict:
    # HealthReport is a dataclass, so asdict works transitively
    data = asdict(report)
    return data


app = FastAPI(title="Drainage Delta Health API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
async def health_check():
    return {"status": "ok"}


@app.post("/api/analyze")
async def analyze(req: AnalyzeRequest):
    if req.demo_mode or os.getenv("DEMO_MODE"):
        try:
            client = FakeDemoClient()
            analyzer = drainage.DeltaLakeAnalyzer(
                client,
                metadata_file_count_threshold=req.metadata_file_count_threshold,
                metadata_total_size_threshold=req.metadata_total_size_threshold,
            )
            report = await analyzer.analyze()
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc
        return _report_to_dict(report)

    client_id = req.client_id or DEFAULT_CLIENT_ID
    try:
        report = await drainage.analyze_delta_lake_async(
            req.path,
            client_id=client_id,
            metadata_file_count_threshold=req.metadata_file_count_threshold,
            metadata_total_size_threshold=req.metadata_total_size_threshold,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    return _report_to_dict(report)


@app.get("/api/demo")
async def demo():
    """Run analysis against built-in demo data without ADLS."""
    try:
        client = FakeDemoClient()
        analyzer = drainage.DeltaLakeAnalyzer(client)
        report = await analyzer.analyze()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return _report_to_dict(report)


# Static UI (served from ./web)
if os.path.isdir("web"):
    app.mount("/ui", StaticFiles(directory="web", html=True), name="ui")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app:app", host="0.0.0.0", port=int(os.getenv("PORT", "8080")), reload=False)
