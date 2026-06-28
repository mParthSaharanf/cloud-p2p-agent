# agent/http_storage_adapter.py
from __future__ import annotations

import hashlib
from typing import AsyncIterator

import httpx

from agent.storage import StorageBackend, StorageError, FileNotFoundError


class HTTPStorageAdapter(StorageBackend):
    """
    StorageBackend implementation that reads files from a remote HTTP
    file server using Range requests — mimicking how a real cloud storage
    API (Google Drive, Dropbox) works under the hood.

    The file server is a simple static HTTP server serving files by hash.
    In production this would be replaced with OAuth-authenticated requests
    to the actual cloud provider API.

    URL structure: http://<server>/<file_hash>
    """

    CHUNK_SIZE = 64 * 1024  # 64 KB per read

    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip("/")
        self._client = httpx.AsyncClient(timeout=30.0)

    async def close(self) -> None:
        await self._client.aclose()

    def _url(self, file_hash: str) -> str:
        return f"{self.base_url}/{file_hash}"

    async def exists(self, file_hash: str) -> bool:
        try:
            resp = await self._client.head(self._url(file_hash))
            return resp.status_code == 200
        except httpx.TransportError:
            return False

    async def get_size(self, file_hash: str) -> int:
        try:
            resp = await self._client.head(self._url(file_hash))
            if resp.status_code == 404:
                raise FileNotFoundError(f"file {file_hash!r} not found on storage server")
            resp.raise_for_status()
            content_length = resp.headers.get("content-length")
            if not content_length:
                raise StorageError(f"storage server did not return Content-Length for {file_hash}")
            return int(content_length)
        except httpx.TransportError as e:
            raise StorageError(f"storage server unreachable: {e}") from e

    async def read_range(
        self,
        file_hash: str,
        start: int,
        end: int,
    ) -> AsyncIterator[bytes]:
        return self._read_range_impl(file_hash, start, end)

    async def _read_range_impl(
        self,
        file_hash: str,
        start: int,
        end: int,
    ) -> AsyncIterator[bytes]:
        """
        Fetch [start, end] inclusive using HTTP Range header.
        Streams the response in CHUNK_SIZE pieces — never buffers the
        full range in memory, which is the zero-disk guarantee.
        """
        headers = {"Range": f"bytes={start}-{end}"}
        try:
            async with self._client.stream(
                "GET", self._url(file_hash), headers=headers
            ) as resp:
                if resp.status_code == 404:
                    raise FileNotFoundError(f"file {file_hash!r} not found")
                if resp.status_code not in (200, 206):
                    raise StorageError(
                        f"storage server returned {resp.status_code} for {file_hash}"
                    )
                async for chunk in resp.aiter_bytes(self.CHUNK_SIZE):
                    yield chunk
        except httpx.TransportError as e:
            raise StorageError(f"storage server unreachable: {e}") from e

    async def write_stream(
        self,
        file_hash: str,
        stream: AsyncIterator[bytes],
    ) -> int:
        """
        Not implemented for HTTP storage — this adapter is read-only.
        Seeding agents write files to the server out of band (via the
        file server's upload endpoint or direct volume mount).
        """
        raise StorageError(
            "HTTPStorageAdapter is read-only — files are pre-loaded on the storage server"
        )

    async def delete(self, file_hash: str) -> None:
        raise StorageError("HTTPStorageAdapter is read-only")

    async def list_files(self) -> list[str]:
        """
        Query the file server's manifest endpoint for available files.
        The file server exposes GET /manifest returning a JSON list of hashes.
        """
        try:
            resp = await self._client.get(f"{self.base_url}/manifest")
            resp.raise_for_status()
            return resp.json()
        except httpx.TransportError as e:
            raise StorageError(f"storage server unreachable: {e}") from e