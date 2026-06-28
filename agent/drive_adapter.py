# agent/drive_adapter.py
from __future__ import annotations

import asyncio
import hashlib
import json as _json
from pathlib import Path
from typing import AsyncIterator, TYPE_CHECKING

import httpx
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

from agent.storage import StorageBackend, StorageError
from agent.storage import FileNotFoundError as P2PFileNotFoundError

if TYPE_CHECKING:
    pass

SCOPES = ["https://www.googleapis.com/auth/drive.file"]
CHUNK_SIZE = 256 * 1024
FOLDER_NAME = "cloud-p2p-agent"


class DriveAdapter(StorageBackend):
    """
    StorageBackend implementation backed by Google Drive.

    Files are stored in a dedicated folder called 'cloud-p2p-agent'
    in the user's Drive, named by their sha256 hash.

    A local manifest maps file_hash → drive_file_id for O(1) lookups
    without hitting the Drive API on every exists() call.

    Upload uses Google's resumable upload API in 256KB chunks.
    Chunks are buffered in memory (not disk) — zero-disk guarantee holds.
    True zero-memory would require knowing total size upfront; documented
    as a known limitation for files > 100MB.
    """

    def __init__(
        self,
        token_path: str,
        credentials_path: str = "config/drive_credentials.json",
    ):
        self.token_path = Path(token_path)
        self.credentials_path = Path(credentials_path)
        self._creds: Credentials | None = None
        self._folder_id: str | None = None
        self._manifest: dict[str, str] = {}
        self._manifest_path = (
            self.token_path.parent
            / f"drive_manifest_{self.token_path.stem}.json"
        )
        self._load_manifest()

    # --- manifest ---

    def _load_manifest(self) -> None:
        if self._manifest_path.exists():
            self._manifest = _json.loads(self._manifest_path.read_text())

    def _save_manifest(self) -> None:
        self._manifest_path.write_text(_json.dumps(self._manifest, indent=2))

    # --- auth ---

    def _get_creds(self) -> Credentials:
        if self._creds and self._creds.valid:
            return self._creds
        creds = Credentials.from_authorized_user_file(
            str(self.token_path), SCOPES
        )
        if creds.expired and creds.refresh_token:
            creds.refresh(Request())
            self.token_path.write_text(creds.to_json())
        self._creds = creds
        return creds

    def _get_service(self):
        return build(
            "drive", "v3",
            credentials=self._get_creds(),
            cache_discovery=False,
        )

    # --- folder ---

    async def _get_folder_id(self) -> str:
        if self._folder_id:
            return self._folder_id

        def _find_or_create():
            service = self._get_service()
            results = service.files().list(
                q=(
                    f"name='{FOLDER_NAME}' and "
                    f"mimeType='application/vnd.google-apps.folder' and "
                    f"trashed=false"
                ),
                fields="files(id, name)",
            ).execute()
            files = results.get("files", [])
            if files:
                return files[0]["id"]
            metadata = {
                "name": FOLDER_NAME,
                "mimeType": "application/vnd.google-apps.folder",
            }
            folder = service.files().create(
                body=metadata, fields="id"
            ).execute()
            return folder["id"]

        self._folder_id = await asyncio.to_thread(_find_or_create)
        return self._folder_id

    # --- StorageBackend interface ---

    async def exists(self, file_hash: str) -> bool:
        return file_hash in self._manifest

    async def get_size(self, file_hash: str) -> int:
        if file_hash not in self._manifest:
            raise P2PFileNotFoundError(
                f"file {file_hash!r} not in Drive manifest"
            )
        file_id = self._manifest[file_hash]

        def _get_size():
            service = self._get_service()
            meta = service.files().get(
                fileId=file_id, fields="size"
            ).execute()
            return int(meta["size"])

        try:
            return await asyncio.to_thread(_get_size)
        except Exception as e:
            raise StorageError(
                f"failed to get size for {file_hash}: {e}"
            ) from e

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
        if file_hash not in self._manifest:
            raise P2PFileNotFoundError(
                f"file {file_hash!r} not in Drive manifest"
            )
        file_id = self._manifest[file_hash]
        creds = self._get_creds()
        url = f"https://www.googleapis.com/drive/v3/files/{file_id}?alt=media"
        headers = {
            "Authorization": f"Bearer {creds.token}",
            "Range": f"bytes={start}-{end}",
        }
        async with httpx.AsyncClient(timeout=60.0) as client:
            async with client.stream("GET", url, headers=headers) as resp:
                if resp.status_code == 404:
                    raise P2PFileNotFoundError(
                        f"file {file_hash!r} not found on Drive"
                    )
                if resp.status_code not in (200, 206):
                    raise StorageError(
                        f"Drive returned {resp.status_code} for {file_hash}"
                    )
                async for chunk in resp.aiter_bytes(CHUNK_SIZE):
                    yield chunk

    async def write_stream(
        self,
        file_hash: str,
        stream: AsyncIterator[bytes],
    ) -> int:
        """
        Upload via Google Drive resumable upload API in 256KB chunks.
        Zero-disk: bytes never touch local disk.
        NOTE: chunks buffered in memory to determine total size for
        Content-Range headers. For files > 100MB consider
        google-resumable-media library for true streaming.
        """
        folder_id = await self._get_folder_id()
        creds = self._get_creds()

        session_uri = await self._initiate_resumable_upload(
            file_hash, folder_id, creds
        )
        total, file_id = await self._stream_chunks_to_drive(
            stream, session_uri, creds
        )

        self._manifest[file_hash] = file_id
        self._save_manifest()
        return total

    async def _initiate_resumable_upload(
        self,
        file_hash: str,
        folder_id: str,
        creds: Credentials,
    ) -> str:
        metadata = _json.dumps({
            "name": file_hash,
            "parents": [folder_id],
        })
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(
                "https://www.googleapis.com/upload/drive/v3/files"
                "?uploadType=resumable&fields=id",
                headers={
                    "Authorization": f"Bearer {creds.token}",
                    "Content-Type": "application/json; charset=UTF-8",
                    "X-Upload-Content-Type": "application/octet-stream",
                },
                content=metadata.encode("utf-8"),
            )
            if resp.status_code != 200:
                raise StorageError(
                    f"failed to initiate resumable upload: "
                    f"{resp.status_code} {resp.text}"
                )
            session_uri = resp.headers.get("Location")
            if not session_uri:
                raise StorageError("Drive did not return a session URI")
            return session_uri

    async def _stream_chunks_to_drive(
        self,
        stream: AsyncIterator[bytes],
        session_uri: str,
        creds: Credentials,
    ) -> tuple[int, str]:
        """
        Buffer stream into 256KB chunks, upload each to Drive.
        Returns (total_bytes, drive_file_id).
        """
        DRIVE_CHUNK = 256 * 1024

        chunks: list[bytes] = []
        leftover = b""

        async for incoming in stream:
            leftover += incoming
            while len(leftover) >= DRIVE_CHUNK:
                chunks.append(leftover[:DRIVE_CHUNK])
                leftover = leftover[DRIVE_CHUNK:]
        if leftover:
            chunks.append(leftover)

        if not chunks:
            raise StorageError("empty stream — nothing to upload")

        total_size = sum(len(c) for c in chunks)
        offset = 0
        file_id = None

        async with httpx.AsyncClient(timeout=60.0) as client:
            for i, chunk in enumerate(chunks):
                is_final = (i == len(chunks) - 1)
                total_str = str(total_size) if is_final else "*"
                content_range = (
                    f"bytes {offset}-{offset + len(chunk) - 1}/{total_str}"
                )
                resp = await client.put(
                    session_uri,
                    headers={
                        "Authorization": f"Bearer {creds.token}",
                        "Content-Range": content_range,
                        "Content-Length": str(len(chunk)),
                    },
                    content=chunk,
                )
                if resp.status_code not in (200, 201, 308):
                    raise StorageError(
                        f"Drive chunk upload failed at offset {offset}: "
                        f"{resp.status_code} {resp.text}"
                    )
                if resp.status_code in (200, 201):
                    file_id = resp.json().get("id")
                offset += len(chunk)

        if not file_id:
            raise StorageError(
                "Drive upload completed but no file ID returned"
            )

        return offset, file_id

    async def delete(self, file_hash: str) -> None:
        if file_hash not in self._manifest:
            return
        file_id = self._manifest[file_hash]

        def _delete():
            service = self._get_service()
            service.files().delete(fileId=file_id).execute()

        await asyncio.to_thread(_delete)
        del self._manifest[file_hash]
        self._save_manifest()

    async def list_files(self) -> list[str]:
        return list(self._manifest.keys())