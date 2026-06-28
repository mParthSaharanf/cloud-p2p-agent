# fileserver/main.py
"""
Simple HTTP file server that mimics a cloud storage API.
Serves files by their sha256 hash with Range request support.
Files are pre-loaded into /files directory named by their hash.
"""
from __future__ import annotations

import hashlib
import os
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import StreamingResponse

FILES_DIR = Path(os.environ.get("FILES_DIR", "/files"))
CHUNK_SIZE = 64 * 1024

app = FastAPI(title="mock-cloud-storage")


def _path(file_hash: str) -> Path:
    return FILES_DIR / file_hash


@app.get("/manifest")
async def manifest() -> list[str]:
    """Return all available file hashes."""
    if not FILES_DIR.exists():
        FILES_DIR.mkdir(parents=True, exist_ok=True)
        return []
    return [p.name for p in FILES_DIR.iterdir() if p.is_file()]

@app.head("/{file_hash}")
async def head_file(file_hash: str) -> Response:
    path = _path(file_hash)
    if not path.exists():
        raise HTTPException(status_code=404)
    size = path.stat().st_size
    return Response(
        headers={
            "Content-Length": str(size),
            "Accept-Ranges": "bytes",
        }
    )


@app.get("/{file_hash}")
async def get_file(file_hash: str, request: Request) -> Response:
    path = _path(file_hash)
    if not path.exists():
        raise HTTPException(status_code=404)

    file_size = path.stat().st_size
    range_header = request.headers.get("Range")

    if range_header:
        # Parse "bytes=start-end"
        try:
            range_val = range_header.replace("bytes=", "")
            start_str, end_str = range_val.split("-")
            start = int(start_str)
            end = int(end_str) if end_str else file_size - 1
        except ValueError:
            raise HTTPException(status_code=416, detail="Invalid Range header")

        if start >= file_size or end >= file_size or start > end:
            raise HTTPException(status_code=416, detail="Range out of bounds")

        length = end - start + 1
        return StreamingResponse(
            _stream_range(path, start, end),
            status_code=206,
            headers={
                "Content-Range": f"bytes {start}-{end}/{file_size}",
                "Content-Length": str(length),
                "Accept-Ranges": "bytes",
            },
            media_type="application/octet-stream",
        )

    # Full file
    return StreamingResponse(
        _stream_range(path, 0, file_size - 1),
        status_code=200,
        headers={
            "Content-Length": str(file_size),
            "Accept-Ranges": "bytes",
        },
        media_type="application/octet-stream",
    )


async def _stream_range(path: Path, start: int, end: int):
    remaining = end - start + 1
    with path.open("rb") as f:
        f.seek(start)
        while remaining > 0:
            chunk = f.read(min(CHUNK_SIZE, remaining))
            if not chunk:
                break
            yield chunk
            remaining -= len(chunk)