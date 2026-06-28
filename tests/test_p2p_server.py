import base64
import hashlib
import json

import httpx
import pytest
from httpx import ASGITransport

from agent.identity import AgentIdentity
from agent.messages import (
    ChunkHeader,
    FileRequestAck,
    FileRequestReject,
    HandshakeRequest,
    HandshakeResponse,
    TransferComplete,
)
from agent.p2p_server import P2PServer
from agent.serializer import decode
from agent.storage import LocalStorage
from agent.trust import TrustEngine


pytestmark = pytest.mark.asyncio


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def server_identity():
    return AgentIdentity.generate()


@pytest.fixture
def client_identity():
    return AgentIdentity.generate()


@pytest.fixture
def server_engine(server_identity, client_identity):
    engine = TrustEngine(identity=server_identity)
    # For tests: client is a direct trust anchor on the server
    engine.trust_anchors.add(client_identity.agent_id)
    return engine


@pytest.fixture
def storage(tmp_path):
    return LocalStorage(tmp_path / "storage")


@pytest.fixture
def server(server_identity, server_engine, storage):
    return P2PServer(
        identity=server_identity,
        trust_engine=server_engine,
        storage=storage,
    )


@pytest.fixture
async def http(server):
    transport = ASGITransport(app=server.app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://testagent"
    ) as client:
        yield client


async def _write_file(storage: LocalStorage, content: bytes) -> str:
    file_hash = hashlib.sha256(content).hexdigest()

    async def stream():
        yield content

    await storage.write_stream(file_hash, stream())
    return file_hash


# ---------------------------------------------------------------------------
# Handshake tests
# ---------------------------------------------------------------------------

async def test_handshake_trusted_peer_succeeds(
    http, client_identity, server_identity
):
    nonce = base64.b64encode(b"test_nonce_bytes").decode("ascii")
    req = HandshakeRequest(
        sender_id=client_identity.agent_id,
        nonce=nonce,
    )
    resp = await http.post("/handshake", content=req.model_dump_json().encode())
    assert resp.status_code == 200

    msg = decode(resp.content)
    assert isinstance(msg, HandshakeResponse)
    assert msg.sender_id == server_identity.agent_id

    # Verify the nonce signature — proves server holds its private key
    from agent.identity import AgentIdentity as AI
    assert AI.verify(server_identity.agent_id, nonce.encode("utf-8"), msg.nonce_signature)


async def test_handshake_untrusted_peer_rejected(http):
    stranger = AgentIdentity.generate()
    nonce = base64.b64encode(b"nonce").decode("ascii")
    req = HandshakeRequest(sender_id=stranger.agent_id, nonce=nonce)
    resp = await http.post("/handshake", content=req.model_dump_json().encode())
    assert resp.status_code == 403


async def test_handshake_malformed_body_rejected(http):
    resp = await http.post("/handshake", content=b"not json at all")
    assert resp.status_code == 400


# ---------------------------------------------------------------------------
# Transfer tests
# ---------------------------------------------------------------------------

async def test_transfer_streams_correct_bytes(
    http, client_identity, storage
):
    content = b"hello from the seeder side"
    file_hash = await _write_file(storage, content)

    from agent.messages import FileRequest
    req = FileRequest(
        file_hash=file_hash,
        range_start=0,
        range_end=len(content) - 1,
        sender_id=client_identity.agent_id,
    )
    resp = await http.post("/transfer", content=req.model_dump_json().encode())
    assert resp.status_code == 200

    # Parse the streaming response frames
    frames = [f for f in resp.content.split(b"\n") if f]
    ack = decode(frames[0])
    assert isinstance(ack, FileRequestAck)
    assert ack.request_id == req.request_id

    header = decode(frames[1])
    assert isinstance(header, ChunkHeader)
    assert header.range_start == 0
    assert header.range_end == len(content) - 1
    assert header.chunk_size == len(content)

    # Raw bytes are everything between header frame and TransferComplete
    raw_bytes = b"".join(frames[2:-1])
    assert raw_bytes == content

    # Verify checksum
    assert hashlib.sha256(raw_bytes).hexdigest() == header.checksum

    complete = decode(frames[-1])
    assert isinstance(complete, TransferComplete)
    assert complete.total_bytes == len(content)


async def test_transfer_partial_range(http, client_identity, storage):
    content = b"abcdefghij"
    file_hash = await _write_file(storage, content)

    from agent.messages import FileRequest
    req = FileRequest(
        file_hash=file_hash,
        range_start=2,
        range_end=5,
        sender_id=client_identity.agent_id,
    )
    resp = await http.post("/transfer", content=req.model_dump_json().encode())
    assert resp.status_code == 200

    frames = [f for f in resp.content.split(b"\n") if f]
    raw_bytes = b"".join(frames[2:-1])
    assert raw_bytes == b"cdef"


async def test_transfer_file_not_found(http, client_identity):
    from agent.messages import FileRequest
    req = FileRequest(
        file_hash="nonexistent_hash",
        range_start=0,
        range_end=100,
        sender_id=client_identity.agent_id,
    )
    resp = await http.post("/transfer", content=req.model_dump_json().encode())
    assert resp.status_code == 403
    msg = decode(resp.content)
    assert isinstance(msg, FileRequestReject)
    assert msg.reason == "file_not_found"


async def test_transfer_untrusted_sender_rejected(http, storage):
    content = b"secret"
    file_hash = await _write_file(storage, content)

    stranger = AgentIdentity.generate()
    from agent.messages import FileRequest
    req = FileRequest(
        file_hash=file_hash,
        range_start=0,
        range_end=len(content) - 1,
        sender_id=stranger.agent_id,
    )
    resp = await http.post("/transfer", content=req.model_dump_json().encode())
    assert resp.status_code == 403
    msg = decode(resp.content)
    assert isinstance(msg, FileRequestReject)
    assert msg.reason == "no_trust_path"


async def test_health_endpoint(http, server_identity):
    resp = await http.get("/health")
    assert resp.status_code == 200
    assert resp.json()["agent_id"] == server_identity.agent_id