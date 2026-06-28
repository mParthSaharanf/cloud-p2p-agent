# tracker/registry.py
from __future__ import annotations

import time
from dataclasses import dataclass, field


@dataclass(frozen=True)
class PeerInfo:
    """
    One peer's presence record for a given file.
    agent_id   — base64 Ed25519 pubkey, canonical identity
    host/port  — where this agent's p2p_server is reachable
    registered_at — epoch seconds, for TTL-based expiry
    """
    agent_id: str
    host: str
    port: int
    registered_at: float = field(default_factory=time.time, compare=False, hash=False)

    def is_expired(self, ttl_seconds: float, now: float | None = None) -> bool:
        if now is None:
            now = time.time()
        return (now - self.registered_at) > ttl_seconds


class Registry:
    """
    In-memory map of file_hash → set[PeerInfo].

    Designed so every mutating operation maps 1:1 to a Redis command
    when persistence is added later:
        register   → HSET + SADD
        unregister → SREM
        get_peers  → SMEMBERS
        list_files → KEYS

    Peers expire passively on read (TTL checked in get_peers) rather than
    via an active background sweep — simpler for a single-process demo,
    and Redis TTL handles it automatically in production.
    """

    DEFAULT_TTL = 300.0  # 5 minutes — peers must re-register to stay live

    def __init__(self, peer_ttl: float = DEFAULT_TTL):
        self.peer_ttl = peer_ttl
        # file_hash → dict[agent_id → PeerInfo]
        # Inner dict keyed by agent_id so re-registration is an O(1) upsert
        # rather than a linear scan through a set.
        self._store: dict[str, dict[str, PeerInfo]] = {}

    def register(self, file_hash: str, peer: PeerInfo) -> None:
        """Add or refresh a peer's presence for a file."""
        if file_hash not in self._store:
            self._store[file_hash] = {}
        # Upsert — replaces the old record, resetting registered_at
        # via a fresh PeerInfo with the same agent_id/host/port.
        self._store[file_hash][peer.agent_id] = PeerInfo(
            agent_id=peer.agent_id,
            host=peer.host,
            port=peer.port,
        )

    def unregister(self, file_hash: str, agent_id: str) -> None:
        """Remove a peer from a file's swarm. No-op if not present."""
        if file_hash in self._store:
            self._store[file_hash].pop(agent_id, None)
            if not self._store[file_hash]:
                del self._store[file_hash]

    def unregister_all(self, agent_id: str) -> None:
        """Remove a peer from every file it was seeding — used on graceful
        shutdown or when a peer sends an explicit leave message."""
        for file_hash in list(self._store):
            self.unregister(file_hash, agent_id)

    def get_peers(
        self,
        file_hash: str,
        now: float | None = None,
    ) -> list[PeerInfo]:
        """Return live (non-expired) peers for a file, pruning stale ones."""
        if file_hash not in self._store:
            return []
        now = now or time.time()
        live, expired = [], []
        for peer in self._store[file_hash].values():
            if peer.is_expired(self.peer_ttl, now):
                expired.append(peer.agent_id)
            else:
                live.append(peer)
        for agent_id in expired:
            self.unregister(file_hash, agent_id)
        return live

    def list_files(self) -> list[str]:
        """Return all file hashes that have at least one registered peer."""
        return list(self._store.keys())

    def peer_count(self, file_hash: str) -> int:
        return len(self._store.get(file_hash, {}))