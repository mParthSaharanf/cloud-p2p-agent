# agent/downloader.py
from __future__ import annotations

import asyncio
import hashlib
import logging
from dataclasses import dataclass, field

import httpx

from agent.identity import AgentIdentity
from agent.messages import (
    ChunkHeader,
    FileRequestAck,
    FileRequestReject,
    TransferComplete,
    FileRequest,
    HandshakeRequest,
    HandshakeResponse,
)
from agent.serializer import decode, decode_as, DeserializationError, encode
from agent.storage import StorageBackend
from agent.tracker_client import PeerAddress, TrackerClient
from agent.trust import TrustEngine

logger = logging.getLogger(__name__)


class DownloadError(Exception):
    """Raised when a download fails unrecoverably."""


class PeerRejectedError(DownloadError):
    """Raised when a peer rejects our request (trust, rate limit, etc)."""


# ---------------------------------------------------------------------------
# Range assignment
# ---------------------------------------------------------------------------

def _split_ranges(
    file_size: int,
    peer_count: int,
    min_chunk: int = 256 * 1024,   # 256 KB minimum per peer
) -> list[tuple[int, int]]:
    """
    Divide [0, file_size) into at most peer_count non-overlapping ranges.
    Returns list of (start, end) inclusive pairs.

    If file_size < min_chunk * peer_count we use fewer peers — no point
    opening 8 connections for a 100 KB file.
    """
    if file_size == 0:
        return []
    actual_peers = min(peer_count, max(1, file_size // min_chunk))
    chunk = file_size // actual_peers
    ranges = []
    for i in range(actual_peers):
        start = i * chunk
        end = (start + chunk - 1) if i < actual_peers - 1 else file_size - 1
        ranges.append((start, end))
    return ranges


# ---------------------------------------------------------------------------
# Per-peer download task
# ---------------------------------------------------------------------------

@dataclass
class ChunkResult:
    peer: PeerAddress
    range_start: int
    range_end: int
    data: bytes
    checksum: str


async def _handshake_peer(
    http: httpx.AsyncClient,
    peer: PeerAddress,
    identity: AgentIdentity,
    trust_engine: TrustEngine,
) -> bool:
    """
    Perform trust handshake with a peer. Returns True if the peer accepted
    us, False if they rejected or are unreachable.
    """
    import base64
    import os

    nonce = base64.b64encode(os.urandom(32)).decode("ascii")

    # Build our trust chain — vouches from trust anchors to our agent_id.
    # For now we send an empty chain and rely on the peer having us as a
    # direct trust anchor (set up at TrustEngine construction time in tests
    # and in config.py for production). Full chain passing is the natural
    # next hardening step.
    req = HandshakeRequest(
        sender_id=identity.agent_id,
        trust_chain=[],
        nonce=nonce,
    )

    try:
        resp = await http.post(
            f"{peer.base_url}/handshake",
            content=encode(req),
            timeout=5.0,
        )
    except httpx.TransportError as e:
        logger.warning("handshake transport error with %s: %s", peer.agent_id[:12], e)
        return False

    if resp.status_code != 200:
        logger.warning(
            "handshake rejected by %s (HTTP %d)", peer.agent_id[:12], resp.status_code
        )
        return False

    try:
        msg = decode_as(resp.content, HandshakeResponse)
    except DeserializationError as e:
        logger.warning("bad handshake response from %s: %s", peer.agent_id[:12], e)
        return False

    assert isinstance(msg, HandshakeResponse)

    # Verify the peer signed our nonce — proves they hold their private key.
    if not AgentIdentity.verify(msg.sender_id, nonce.encode("utf-8"), msg.nonce_signature):
        logger.warning(
            "nonce signature verification failed for %s", peer.agent_id[:12]
        )
        return False

    # Add peer as a trust anchor so subsequent transfer requests pass.
    trust_engine.trust_anchors.add(msg.sender_id)
    logger.debug("handshake succeeded with %s", peer.agent_id[:12])
    return True


async def _fetch_chunk(
    http: httpx.AsyncClient,
    peer: PeerAddress,
    identity: AgentIdentity,
    file_hash: str,
    range_start: int,
    range_end: int,
) -> ChunkResult:
    """
    Request one byte range from one peer. Parses the streaming response
    frames (Ack → ChunkHeader → raw bytes → TransferComplete) and returns
    a ChunkResult. Raises DownloadError on any failure.
    """
    req = FileRequest(
        file_hash=file_hash,
        range_start=range_start,
        range_end=range_end,
        sender_id=identity.agent_id,
    )

    try:
        resp = await http.post(
            f"{peer.base_url}/transfer",
            content=encode(req),
            timeout=30.0,
        )
    except httpx.TransportError as e:
        raise DownloadError(f"transfer transport error with {peer.agent_id[:12]}: {e}") from e

    if resp.status_code == 403:
        try:
            msg = decode(resp.content)
            if isinstance(msg, FileRequestReject):
                raise PeerRejectedError(
                    f"peer {peer.agent_id[:12]} rejected: {msg.reason} — {msg.detail}"
                )
        except DeserializationError:
            pass
        raise PeerRejectedError(f"peer {peer.agent_id[:12]} returned 403")

    if resp.status_code != 200:
        raise DownloadError(
            f"peer {peer.agent_id[:12]} returned HTTP {resp.status_code}"
        )

    # Parse response frames split by newline delimiters
    # Parse response frames using chunk_size to bound raw bytes precisely.
    # Splitting by \n is wrong — raw bytes can contain newlines.
    try:
        content = resp.content

        # Frame 1: FileRequestAck (ends at first \n)
        idx = content.index(b"\n")
        ack = decode_as(content[:idx], FileRequestAck)
        assert isinstance(ack, FileRequestAck)

        # Frame 2: ChunkHeader (ends at second \n)
        idx2 = content.index(b"\n", idx + 1)
        header = decode_as(content[idx + 1:idx2], ChunkHeader)
        assert isinstance(header, ChunkHeader)

        # Raw bytes: exactly chunk_size bytes after the second \n
        raw_start = idx2 + 1
        raw_bytes = content[raw_start:raw_start + header.chunk_size]

        # Frame 4: TransferComplete (after raw bytes + \n delimiter)
        complete_start = raw_start + header.chunk_size + 1
        complete = decode_as(content[complete_start:], TransferComplete)
        assert isinstance(complete, TransferComplete)

    except (ValueError, DeserializationError) as e:
        raise DownloadError(f"bad frame from {peer.agent_id[:12]}: {e}") from e
    
    # Verify checksum
    actual_checksum = hashlib.sha256(raw_bytes).hexdigest()
    if actual_checksum != header.checksum:
        raise DownloadError(
            f"checksum mismatch from {peer.agent_id[:12]}: "
            f"expected {header.checksum}, got {actual_checksum}"
        )

    return ChunkResult(
        peer=peer,
        range_start=header.range_start,
        range_end=header.range_end,
        data=raw_bytes,
        checksum=header.checksum,
    )


# ---------------------------------------------------------------------------
# Downloader
# ---------------------------------------------------------------------------

@dataclass
class DownloadResult:
    file_hash: str
    total_bytes: int
    peers_used: list[str]       # agent_ids
    verified: bool              # final sha256 matched expected hash


class Downloader:
    """
    Orchestrates concurrent multi-peer downloads.

    For each download:
      1. Query tracker for peers seeding the file
      2. Handshake with each candidate peer concurrently
      3. Split the file into ranges, one per trusted peer
      4. Fetch all ranges concurrently via asyncio.gather
      5. Reassemble chunks in order and write to destination storage
      6. Verify final sha256 against expected file_hash

    The zero-disk guarantee holds at the storage layer: each chunk is
    held in memory only long enough to be written to the destination
    StorageBackend, then discarded.
    """

    def __init__(
        self,
        identity: AgentIdentity,
        trust_engine: TrustEngine,
        tracker: TrackerClient,
        storage: StorageBackend,
        max_peers: int = 4,
        timeout: float = 30.0,
    ):
        self.identity = identity
        self.trust_engine = trust_engine
        self.tracker = tracker
        self.storage = storage
        self.max_peers = max_peers
        self.timeout = timeout

    async def download(self, file_hash: str, file_size: int) -> DownloadResult:
        """
        Download a file by hash from the swarm.
        file_size must be known ahead of time (from tracker metadata or
        a prior HEAD-style query — tracker will carry this in a later step).
        """
        logger.info("starting download of %s (%d bytes)", file_hash[:12], file_size)

        # 1. Discover peers
        candidates = await self.tracker.get_peers(file_hash)
        if not candidates:
            raise DownloadError(f"no peers found for {file_hash[:12]}")

        candidates = candidates[: self.max_peers]

        # 2. Handshake concurrently
        async with httpx.AsyncClient() as http:
            trusted_peers = await self._handshake_all(http, candidates)
            if not trusted_peers:
                raise DownloadError("no peers passed trust handshake")

            # 3. Split ranges
            ranges = _split_ranges(file_size, len(trusted_peers))
            peer_range_pairs = list(zip(trusted_peers, ranges))

            # 4. Fetch all ranges concurrently
            tasks = [
                _fetch_chunk(http, peer, self.identity, file_hash, start, end)
                for peer, (start, end) in peer_range_pairs
            ]
            results: list[ChunkResult | BaseException] = await asyncio.gather(
                *tasks, return_exceptions=True
            )

        # Separate successes from failures
        chunks: list[ChunkResult] = []
        for i, result in enumerate(results):
            if isinstance(result, BaseException):
                logger.warning(
                    "chunk %d failed from %s: %s",
                    i,
                    peer_range_pairs[i][0].agent_id[:12],
                    result,
                )
            else:
                chunks.append(result)

        if not chunks:
            raise DownloadError("all chunk fetches failed")

        # Check we got contiguous coverage — if any chunk failed we can't
        # reassemble. Full retry-with-fallback-peer logic is a natural
        # extension but out of scope for the portfolio demo.
        chunks.sort(key=lambda c: c.range_start)
        expected_start = 0
        for chunk in chunks:
            if chunk.range_start != expected_start:
                raise DownloadError(
                    f"gap in coverage: expected range starting at {expected_start}, "
                    f"got {chunk.range_start}"
                )
            expected_start = chunk.range_end + 1

        if expected_start != file_size:
            raise DownloadError(
                f"incomplete download: got {expected_start}/{file_size} bytes"
            )

        # 5. Write reassembled chunks to storage
        total_bytes = sum(len(c.data) for c in chunks)

        async def reassembled_stream():
            for chunk in chunks:
                yield chunk.data

        await self.storage.write_stream(file_hash, reassembled_stream())

        # 6. Verify final hash
        byte_stream = await self.storage.read_range(file_hash, 0, file_size - 1)
        final_hash = hashlib.sha256()
        async for chunk in byte_stream:
            final_hash.update(chunk)
        verified = final_hash.hexdigest() == file_hash

        if not verified:
            logger.error("final hash mismatch for %s — discarding", file_hash[:12])
            await self.storage.delete(file_hash)
            raise DownloadError(f"final hash verification failed for {file_hash}")

        peers_used = [c.peer.agent_id for c in chunks]
        logger.info(
            "download complete: %s (%d bytes, %d peers, verified=%s)",
            file_hash[:12], total_bytes, len(peers_used), verified,
        )
        return DownloadResult(
            file_hash=file_hash,
            total_bytes=total_bytes,
            peers_used=peers_used,
            verified=verified,
        )

    async def _handshake_all(
        self,
        http: httpx.AsyncClient,
        candidates: list[PeerAddress],
    ) -> list[PeerAddress]:
        """Handshake with all candidates concurrently, return only trusted ones."""
        tasks = [
            _handshake_peer(http, peer, self.identity, self.trust_engine)
            for peer in candidates
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        trusted = []
        for peer, result in zip(candidates, results):
            if result is True:
                trusted.append(peer)
            else:
                logger.debug(
                    "peer %s failed handshake: %s", peer.agent_id[:12], result
                )
        return trusted