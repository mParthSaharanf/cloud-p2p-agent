# agent/main.py
from __future__ import annotations

import os
import asyncio
import logging
import signal
from pathlib import Path

import uvicorn

from agent.config import load_config
from agent.identity import AgentIdentity
from agent.storage import LocalStorage
from agent.tracker_client import TrackerClient
from agent.trust import TrustEngine

logger = logging.getLogger(__name__)

KEEPALIVE_INTERVAL = 120.0  # seconds between tracker re-registrations


async def run(config_path: str = "config/settings.yaml") -> None:
    cfg = load_config(config_path)

    # Identity
    identity = AgentIdentity.load_or_create(cfg.trust.identity_path)
    logger.info("agent identity: %s", identity.agent_id[:16])

    # Trust engine
    trust_engine = TrustEngine(
        identity=identity,
        trust_anchors=set(cfg.trust.anchors),
        max_depth=cfg.trust.max_depth,
    )

    # Storage
    if cfg.storage.use_drive and cfg.storage.drive_token_path:
        from agent.drive_adapter import DriveAdapter
        storage = DriveAdapter(cfg.storage.drive_token_path)
        logger.info("using Google Drive storage backend")
    elif cfg.storage.use_http and cfg.storage.server_url:
        from agent.http_storage_adapter import HTTPStorageAdapter
        storage = HTTPStorageAdapter(cfg.storage.server_url)
        logger.info("using HTTP storage backend: %s", cfg.storage.server_url)
    else:
        storage = LocalStorage(cfg.storage.root)

    advertise_host = os.environ.get("P2P_ADVERTISE_HOST", cfg.server.host)
    # Tracker client
    tracker = TrackerClient(
        tracker_url=cfg.tracker.url,
        agent_id=identity.agent_id,
        host=advertise_host,
        port=cfg.server.port,
    )

    # P2P server — imported here to avoid circular imports at module level
    from agent.p2p_server import P2PServer
    p2p_server = P2PServer(
        identity=identity,
        trust_engine=trust_engine,
        storage=storage,
    )

    # Register with tracker
    seeded = await storage.list_files()
    if seeded:
        await tracker.register(seeded)
        logger.info("registered %d files with tracker", len(seeded))

    # Graceful shutdown
    shutdown_event = asyncio.Event()

    def _handle_signal():
        logger.info("shutdown signal received")
        shutdown_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _handle_signal)

    # Keepalive loop
    async def keepalive_loop():
        while not shutdown_event.is_set():
            try:
                await asyncio.wait_for(
                    asyncio.shield(shutdown_event.wait()),
                    timeout=KEEPALIVE_INTERVAL,
                )
                break  # shutdown was set
            except asyncio.TimeoutError:
                files = await storage.list_files()
                if files:
                    await tracker.keepalive(files)
                    logger.debug("keepalive sent for %d files", len(files))

    # Uvicorn config — run p2p server
    uv_config = uvicorn.Config(
        app=p2p_server.app,
        host=cfg.server.host,
        port=cfg.server.port,
        log_level="warning",
    )
    server = uvicorn.Server(uv_config)

    # Run uvicorn + keepalive concurrently until shutdown
    try:
        await asyncio.gather(
            server.serve(),
            keepalive_loop(),
        )
    finally:
        server.should_exit = True
        await tracker.unregister()
        await tracker.close()
        logger.info("agent shut down cleanly")


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    asyncio.run(run())


if __name__ == "__main__":
    main()