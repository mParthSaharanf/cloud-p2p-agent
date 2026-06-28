import pytest
import time

from fastapi.testclient import TestClient

from tracker.main import app, registry
from tracker.registry import PeerInfo, Registry


# ---------------------------------------------------------------------------
# Registry unit tests
# ---------------------------------------------------------------------------

def make_peer(agent_id: str = "agent_a", host: str = "1.2.3.4", port: int = 8000):
    return PeerInfo(agent_id=agent_id, host=host, port=port)


def test_register_and_get_peers():
    r = Registry()
    r.register("hash1", make_peer("a"))
    peers = r.get_peers("hash1")
    assert len(peers) == 1
    assert peers[0].agent_id == "a"


def test_register_refreshes_existing_peer():
    r = Registry()
    r.register("hash1", make_peer("a"))
    r.register("hash1", make_peer("a"))  # re-register same peer
    assert r.peer_count("hash1") == 1


def test_unregister_removes_peer():
    r = Registry()
    r.register("hash1", make_peer("a"))
    r.unregister("hash1", "a")
    assert r.get_peers("hash1") == []


def test_unregister_all_removes_from_all_files():
    r = Registry()
    r.register("hash1", make_peer("a"))
    r.register("hash2", make_peer("a"))
    r.unregister_all("a")
    assert r.get_peers("hash1") == []
    assert r.get_peers("hash2") == []


def test_expired_peers_pruned_on_read():
    r = Registry(peer_ttl=10.0)
    r.register("hash1", make_peer("a"))
    future = time.time() + 9999
    peers = r.get_peers("hash1", now=future)
    assert peers == []
    assert r.peer_count("hash1") == 0


def test_unexpired_peers_returned():
    r = Registry(peer_ttl=300.0)
    r.register("hash1", make_peer("a"))
    peers = r.get_peers("hash1", now=time.time() + 1)
    assert len(peers) == 1


def test_list_files():
    r = Registry()
    r.register("hash1", make_peer("a"))
    r.register("hash2", make_peer("b"))
    assert set(r.list_files()) == {"hash1", "hash2"}


def test_unknown_file_returns_empty():
    r = Registry()
    assert r.get_peers("nonexistent") == []


def test_unregister_last_peer_removes_file_entry():
    r = Registry()
    r.register("hash1", make_peer("a"))
    r.unregister("hash1", "a")
    assert "hash1" not in r.list_files()


# ---------------------------------------------------------------------------
# FastAPI integration tests
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def clear_registry():
    """Reset shared registry state between tests."""
    registry._store.clear()
    yield
    registry._store.clear()


@pytest.fixture
def client():
    return TestClient(app)


def test_register_endpoint(client):
    resp = client.post("/register", json={
        "agent_id": "agent_a",
        "host": "1.2.3.4",
        "port": 8000,
        "file_hashes": ["hash1", "hash2"],
    })
    assert resp.status_code == 200
    assert resp.json()["registered"] == 2


def test_get_peers_endpoint(client):
    client.post("/register", json={
        "agent_id": "agent_a",
        "host": "1.2.3.4",
        "port": 8000,
        "file_hashes": ["hash1"],
    })
    resp = client.get("/peers/hash1")
    assert resp.status_code == 200
    data = resp.json()
    assert data["file_hash"] == "hash1"
    assert len(data["peers"]) == 1
    assert data["peers"][0]["agent_id"] == "agent_a"


def test_get_peers_returns_empty_for_unknown_file(client):
    resp = client.get("/peers/unknown_hash")
    assert resp.status_code == 200
    assert resp.json()["peers"] == []


def test_unregister_specific_files(client):
    client.post("/register", json={
        "agent_id": "agent_a",
        "host": "1.2.3.4",
        "port": 8000,
        "file_hashes": ["hash1", "hash2"],
    })
    client.post("/unregister", json={
        "agent_id": "agent_a",
        "file_hashes": ["hash1"],
    })
    assert len(client.get("/peers/hash1").json()["peers"]) == 0
    assert len(client.get("/peers/hash2").json()["peers"]) == 1


def test_unregister_all_files(client):
    client.post("/register", json={
        "agent_id": "agent_a",
        "host": "1.2.3.4",
        "port": 8000,
        "file_hashes": ["hash1", "hash2"],
    })
    client.post("/unregister", json={"agent_id": "agent_a"})
    assert len(client.get("/peers/hash1").json()["peers"]) == 0
    assert len(client.get("/peers/hash2").json()["peers"]) == 0


def test_register_empty_file_hashes_rejected(client):
    resp = client.post("/register", json={
        "agent_id": "agent_a",
        "host": "1.2.3.4",
        "port": 8000,
        "file_hashes": [],
    })
    assert resp.status_code == 422


def test_health_endpoint(client):
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


def test_list_files_endpoint(client):
    client.post("/register", json={
        "agent_id": "agent_a",
        "host": "1.2.3.4",
        "port": 8000,
        "file_hashes": ["hash1"],
    })
    resp = client.get("/files")
    assert resp.status_code == 200
    assert "hash1" in resp.json()