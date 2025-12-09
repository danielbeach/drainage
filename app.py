import os
import asyncio
from dataclasses import asdict
from typing import Optional
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, HttpUrl, validator

import delta_skelter

DEFAULT_CLIENT_ID = os.getenv("UAMI_DEFAULT_CLIENT_ID", "UAMI_DEV_NEU_AT42938_AKS_RT")


class AnalyzeRequest(BaseModel):
    path: str = Field(..., description="ADLS Delta Lake path (abfss or https)")
    client_id: Optional[str] = Field(None, description="Override UAMI client id")
    metadata_file_count_threshold: Optional[int] = None
    metadata_total_size_threshold: Optional[int] = None

    @validator("path")
    def validate_path(cls, v: str) -> str:
        if not (v.startswith("abfss://") or v.startswith("https://")):
            raise ValueError("Path must start with abfss:// or https://")
        return v


class FakeDemoClient:
    """Lightweight in-memory client for demo scenarios."""

    def __init__(self, profile: str):
        self._filesystem = "demo-fs"
        self._account = "demo-account"
        self._prefix = profile.replace(" ", "-")
        self._objects, self._content_map = self._build(profile)

    def _build(self, profile: str):
        def obj(key: str, size: int):
            return type("O", (), {"key": key, "size": size, "last_modified": None})

        if profile == "very-well-housekept":
            data = [
                obj("data/part-00000.parquet", 8 * 1024 * 1024),
                obj("data/part-00001.parquet", 9 * 1024 * 1024),
            ]
            metadata = [obj("_delta_log/00000.json", 400)]
            content = {
                "_delta_log/00000.json": b'{"add": {"path": "demo-fs/data/part-00000.parquet"}}\n{"add": {"path": "demo-fs/data/part-00001.parquet"}}'
            }
        if profile == "very-well-housekept":
            data = [
                obj("data/part-00000.parquet", 8 * 1024 * 1024),
                obj("data/part-00001.parquet", 9 * 1024 * 1024),
            ]
            metadata = [obj("_delta_log/00000.json", 400)]
            content = {
                "_delta_log/00000.json": b'{"add": {"path": "data/part-00000.parquet"}}\n{"add": {"path": "data/part-00001.parquet"}}'
            }
        elif profile == "quite-well-housekept":
            data = [
                obj("data/part-00000.parquet", 6 * 1024 * 1024),
                obj("data/part-00001.parquet", 10 * 1024 * 1024),
                obj("data/unref-00002.parquet", 2 * 1024 * 1024),
            ]
            metadata = [
                obj("_delta_log/00000.json", 450),
                obj("_delta_log/00001.checkpoint.parquet", 3 * 1024 * 1024),
            ]
            content = {
                "_delta_log/00000.json": b'{"add": {"path": "data/part-00000.parquet"}}'
            }
        elif profile == "extremely-poorly-housekept":
            data = [
                obj(f"data/unref-{i:05d}.parquet", 1 * 1024 * 1024) for i in range(5)
            ] + [obj("data/part-00000.parquet", 1 * 1024 * 1024)]
            metadata = [obj(f"_delta_log/{i:05d}.json", 600) for i in range(5)] + [
                obj("_delta_log/00005.checkpoint.parquet", 5 * 1024 * 1024)
            ]
            content = {
                "_delta_log/00000.json": b'{"add": {"path": "data/part-00000.parquet"}}'
            }
        return data + metadata, content

    def get_bucket(self):
        return self._filesystem

    def get_prefix(self):
        return self._prefix

    def get_account(self):
        return self._account

    async def list_objects(self, prefix=None):
        return self._objects

    async def get_object(self, key: str):
        return self._content_map.get(key, b"")


def _report_to_dict(report: delta_skelter.HealthReport) -> dict:
    # HealthReport is a dataclass, so asdict works transitively
    data = asdict(report)
    return data


app = FastAPI(title="Delta-Skelter Delta Health API", version="1.0.0")

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
    client_id = req.client_id or DEFAULT_CLIENT_ID
    try:
        report = await delta_skelter.analyze_delta_lake_async(
            req.path,
            client_id=client_id,
            metadata_file_count_threshold=req.metadata_file_count_threshold,
            metadata_total_size_threshold=req.metadata_total_size_threshold,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    return _report_to_dict(report)


@app.get("/api/demo/{profile}")
async def demo(profile: str):
    """Run analysis against built-in demo data without ADLS."""
    if profile not in {
        "very-well-housekept",
        "quite-well-housekept",
        "extremely-poorly-housekept",
    }:
        raise HTTPException(status_code=400, detail="Unknown demo profile")

    try:
        client = FakeDemoClient(profile)
        analyzer = delta_skelter.DeltaLakeAnalyzer(client)
        report = await analyzer.analyze()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return _report_to_dict(report)


# Static UI (served from ./web)
if os.path.isdir("web"):
    app.mount("/ui", StaticFiles(directory="web", html=True), name="ui")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "app:app", host="0.0.0.0", port=int(os.getenv("PORT", "8080")), reload=False
    )
