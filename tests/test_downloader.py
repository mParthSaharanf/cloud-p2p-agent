import hashlib
import pytest
import httpx
from httpx import ASGITransport
from unittest.mock import AsyncMock, MagicMock

from agent.downloader import (
    Downloader,
    DownloadError,
    ChunkResult,
    _split_ranges,
    _fetch_chunk,
    _handshake_peer,
)
from agent.identity import AgentIdentity
from agent.p2p_server import P2PServer
from agent.storage import LocalStorage
from agent.tracker_client import PeerAddress, TrackerClient
from agent.trust import TrustEngine


pytestmark = pytest.mark.asyncio


# ---------------------------------------------------------------------------
# _split_ranges unit tests
# ---------------------------------------------------------------------------

def test_split_ranges_single_peer():
    ranges = _split_ranges(1000, 1)
    assert ranges == [(0, 999)]


def test_split_ranges_two_peers():
    ranges = _split_ranges(1000, 2, min_chunk=1)
    assert ranges[0][0] == 0
    assert ranges[-1][1] == 999
    # No gaps
    for i in range(len(ranges) - 1):
        assert ranges[i][1] + 1 == ranges[i + 1][0]


def test_split_ranges_fewer_peers_for_small_file():
    # File smaller than min_chunk * peer_count → fewer peers used
    ranges = _split_ranges(100, 4, min_chunk=256 * 1024)
    assert len(ranges) == 1


def test_split_ranges_empty_file():
    assert _split_ranges(0, 4) == []


def test_split_ranges_covers_full_file():
    file_size = 10 * 1024 * 1024  # 10 MB
    ranges = _split_ranges(file_size, 4, min_chunk=1)
    assert ranges[0][0] == 0
    assert ranges[-1][1] == file_size - 1


# ---------------------------------------------------------------------------
# Fixtures — two in-process agents (server + client)
# ---------------------------------------------------------------------------

@pytest.fixture
def server_identity():
    return AgentIdentity.generate()


@pytest.fixture
def client_identity():
    return AgentIdentity.generate()


@pytest.fixture
def server_storage(tmp_path):
    return LocalStorage(tmp_path / "server_storage")


@pytest.fixture
def client_storage(tmp_path):
    return LocalStorage(tmp_path / "client_storage")


@pytest.fixture
def server_engine(server_identity, client_identity):
    engine = TrustEngine(identity=server_identity)
    engine.trust_anchors.add(client_identity.agent_id)
    return engine


@pytest.fixture
def client_engine(client_identity, server_identity):
    engine = TrustEngine(identity=client_identity)
    engine.trust_anchors.add(server_identity.agent_id)
    return engine


@pytest.fixture
def p2p_server(server_identity, server_engine, server_storage):
    return P2PServer(
        identity=server_identity,
        trust_engine=server_engine,
        storage=server_storage,
    )


async def _seed_file(storage: LocalStorage, content: bytes) -> str:
    file_hash = hashlib.sha256(content).hexdigest()

    async def stream():
        yield content

    await storage.write_stream(file_hash, stream())
    return file_hash


# ---------------------------------------------------------------------------
# _fetch_chunk + _handshake_peer integration tests
# ---------------------------------------------------------------------------

async def test_handshake_and_fetch_chunk(
    p2p_server, server_identity, client_identity, client_engine, server_storage
):
    content = b"chunk content for testing"
    file_hash = await _seed_file(server_storage, content)

    transport = ASGITransport(app=p2p_server.app)
    peer = PeerAddress(
        agent_id=server_identity.agent_id, host="testagent", port=0
    )

    async with httpx.AsyncClient(
        transport=transport, base_url="http://testagent"
    ) as http:
        # Handshake first
        ok = await _handshake_peer(http, peer, client_identity, client_engine)
        assert ok is True

        # Now fetch
        result = await _fetch_chunk(
            http, peer, client_identity,
            file_hash, 0, len(content) - 1
        )

    assert result.data == content
    assert hashlib.sha256(result.data).hexdigest() == result.checksum


async def test_fetch_chunk_checksum_integrity(
    p2p_server, server_identity, client_identity, client_engine, server_storage
):
    content = b"x" * 4096
    file_hash = await _seed_file(server_storage, content)

    transport = ASGITransport(app=p2p_server.app)
    peer = PeerAddress(agent_id=server_identity.agent_id, host="testagent", port=0)

    async with httpx.AsyncClient(
        transport=transport, base_url="http://testagent"
    ) as http:
        await _handshake_peer(http, peer, client_identity, client_engine)
        result = await _fetch_chunk(
            http, peer, client_identity,
            file_hash, 0, len(content) - 1
        )

    assert result.data == content


# ---------------------------------------------------------------------------
# Full Downloader integration test
# ---------------------------------------------------------------------------

async def test_downloader_single_peer(
    p2p_server, server_identity, client_identity,
    client_engine, server_storage, client_storage, tmp_path
):
    content = b"full file download test content " * 100
    file_hash = await _seed_file(server_storage, content)

    transport = ASGITransport(app=p2p_server.app)
    peer = PeerAddress(
        agent_id=server_identity.agent_id, host="testagent", port=0
    )

    # Mock tracker to return our in-process peer
    mock_tracker = MagicMock(spec=TrackerClient)
    mock_tracker.get_peers = AsyncMock(return_value=[peer])

    downloader = Downloader(
        identity=client_identity,
        trust_engine=client_engine,
        tracker=mock_tracker,
        storage=client_storage,
        max_peers=1,
    )

    # Patch httpx.AsyncClient to use ASGITransport
    original_init = httpx.AsyncClient.__init__

    class PatchedClient(httpx.AsyncClient):
        def __init__(self, **kwargs):
            kwargs["transport"] = transport
            kwargs["base_url"] = "http://testagent"
            super().__init__(**kwargs)

    import agent.downloader as dl_module
    original_client = dl_module.httpx.AsyncClient
    dl_module.httpx.AsyncClient = PatchedClient

    try:
        result = await downloader.download(file_hash, len(content))
    finally:
        dl_module.httpx.AsyncClient = original_client

    assert result.verified is True
    assert result.total_bytes == len(content)
    assert result.file_hash == file_hash
    assert await client_storage.exists(file_hash)


async def test_downloader_no_peers_raises(
    client_identity, client_engine, client_storage
):
    mock_tracker = MagicMock(spec=TrackerClient)
    mock_tracker.get_peers = AsyncMock(return_value=[])

    downloader = Downloader(
        identity=client_identity,
        trust_engine=client_engine,
        tracker=mock_tracker,
        storage=client_storage,
    )

    with pytest.raises(DownloadError, match="no peers found"):
        await downloader.download("somehash", 1024)