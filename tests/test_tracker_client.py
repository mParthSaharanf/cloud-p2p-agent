import pytest
import httpx
from httpx import AsyncClient, ASGITransport

from tracker.main import app, registry
from agent.tracker_client import TrackerClient, TrackerError, PeerAddress


pytestmark = pytest.mark.asyncio


@pytest.fixture(autouse=True)
def clear_registry():
    registry._store.clear()
    yield
    registry._store.clear()


@pytest.fixture
async def client():
    """TrackerClient wired directly to the tracker ASGI app — no real
    network socket needed. ASGITransport lets httpx talk to a FastAPI app
    in-process, which keeps tests fast and self-contained."""
    transport = ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://testtracker"
    ) as http:
        tc = TrackerClient(
            tracker_url="http://testtracker",
            agent_id="agent_test",
            host="127.0.0.1",
            port=9000,
        )
        tc._client = http
        yield tc


async def test_register_returns_count(client):
    count = await client.register(["hash1", "hash2"])
    assert count == 2


async def test_get_peers_returns_other_agents(client):
    # Register a different agent directly via the tracker app
    transport = ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://testtracker"
    ) as http:
        await http.post("/register", json={
            "agent_id": "other_agent",
            "host": "2.2.2.2",
            "port": 8001,
            "file_hashes": ["hash1"],
        })

    peers = await client.get_peers("hash1")
    assert len(peers) == 1
    assert peers[0].agent_id == "other_agent"
    assert isinstance(peers[0], PeerAddress)


async def test_get_peers_excludes_self(client):
    # Register self
    await client.register(["hash1"])
    peers = await client.get_peers("hash1")
    # Self should be excluded
    assert all(p.agent_id != "agent_test" for p in peers)


async def test_get_peers_empty_for_unknown_file(client):
    peers = await client.get_peers("unknown_hash")
    assert peers == []


async def test_unregister_does_not_raise(client):
    await client.register(["hash1"])
    await client.unregister(["hash1"])
    peers = await client.get_peers("hash1")
    assert peers == []


async def test_unregister_all(client):
    await client.register(["hash1", "hash2"])
    await client.unregister(None)
    assert await client.get_peers("hash1") == []
    assert await client.get_peers("hash2") == []


async def test_keepalive_refreshes_registration(client):
    await client.register(["hash1"])
    # keepalive is just re-register — should not raise
    await client.keepalive(["hash1"])
    peers = await client.get_peers("hash1")
    # self excluded, but registration succeeded means no error
    assert isinstance(peers, list)


async def test_tracker_error_on_bad_status():
    transport = ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://testtracker"
    ) as http:
        tc = TrackerClient(
            tracker_url="http://testtracker",
            agent_id="agent_test",
            host="127.0.0.1",
            port=9000,
        )
        tc._client = http
        with pytest.raises(TrackerError):
            # empty file_hashes returns 422 from the tracker
            await tc.register([])