# tracker/main.py
from __future__ import annotations

from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel

from tracker.registry import PeerInfo, Registry

app = FastAPI(title="cloud-p2p-agent tracker", version="0.1.0")
registry = Registry()


# ---------------------------------------------------------------------------
# Request / response schemas
# ---------------------------------------------------------------------------

class RegisterRequest(BaseModel):
    agent_id: str
    host: str
    port: int
    file_hashes: list[str]   # one registration call announces all seeded files


class RegisterResponse(BaseModel):
    registered: int          # how many file entries were created/refreshed


class PeerResponse(BaseModel):
    agent_id: str
    host: str
    port: int


class PeersResponse(BaseModel):
    file_hash: str
    peers: list[PeerResponse]


class UnregisterRequest(BaseModel):
    agent_id: str
    file_hashes: list[str] | None = None  # None means "all files"


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.post("/register", response_model=RegisterResponse, status_code=status.HTTP_200_OK)
async def register(req: RegisterRequest) -> RegisterResponse:
    """
    Agent announces itself as a seeder for one or more files.
    Called on startup and periodically as a keepalive (re-registration
    resets the TTL clock, so peers that stop re-registering expire
    automatically within peer_ttl seconds).
    """
    if not req.file_hashes:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="file_hashes must not be empty",
        )
    peer = PeerInfo(agent_id=req.agent_id, host=req.host, port=req.port)
    for file_hash in req.file_hashes:
        registry.register(file_hash, peer)
    return RegisterResponse(registered=len(req.file_hashes))


@app.get("/peers/{file_hash}", response_model=PeersResponse)
async def get_peers(file_hash: str) -> PeersResponse:
    """
    Return all live peers currently seeding a file.
    Expired peers are pruned on read so the response never includes stale
    addresses — the downloader can use this list directly without further
    liveness checks.
    """
    peers = registry.get_peers(file_hash)
    return PeersResponse(
        file_hash=file_hash,
        peers=[
            PeerResponse(agent_id=p.agent_id, host=p.host, port=p.port)
            for p in peers
        ],
    )


@app.post("/unregister", status_code=status.HTTP_200_OK)
async def unregister(req: UnregisterRequest) -> dict:
    """
    Agent leaves the swarm — either for specific files or all files.
    Called on graceful shutdown; the tracker also expires peers passively
    via TTL so this endpoint isn't strictly required for correctness.
    """
    if req.file_hashes is None:
        registry.unregister_all(req.agent_id)
    else:
        for file_hash in req.file_hashes:
            registry.unregister(file_hash, req.agent_id)
    return {}


@app.get("/files", response_model=list[str])
async def list_files() -> list[str]:
    """Return all file hashes currently tracked. Useful for debugging."""
    return registry.list_files()


@app.get("/health")
async def health() -> dict:
    return {"status": "ok"}