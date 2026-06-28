# agent/cli.py
"""
Manual CLI commands for development and debugging.
Usage:
    python -m agent.cli <command> [args]

Commands:
    seed <file_path>         Hash a file and copy it into local storage
    download <file_hash> <file_size>   Download a file from the swarm
    peers <file_hash>        List peers seeding a file
    identity                 Print this agent's identity (pubkey)
    trust-add <agent_id>     Add an agent_id as a local trust anchor
    files                    List files in local storage
"""
from __future__ import annotations

import asyncio
import hashlib
import sys
from pathlib import Path


async def cmd_seed(file_path: str, storage_root: str = "./data") -> None:
    from agent.storage import LocalStorage
    storage = LocalStorage(storage_root)
    path = Path(file_path)
    if not path.exists():
        print(f"error: file not found: {file_path}", file=sys.stderr)
        sys.exit(1)

    content = path.read_bytes()
    file_hash = hashlib.sha256(content).hexdigest()

    async def stream():
        yield content

    await storage.write_stream(file_hash, stream())
    print(f"seeded: {file_hash}")
    print(f"size:   {len(content)} bytes")


async def cmd_download(
    file_hash: str,
    file_size: int,
    config_path: str = "config/settings.yaml",
) -> None:
    from agent.config import load_config
    from agent.downloader import Downloader
    from agent.identity import AgentIdentity
    from agent.storage import LocalStorage
    from agent.tracker_client import TrackerClient
    from agent.trust import TrustEngine

    cfg = load_config(config_path)
    identity = AgentIdentity.load_or_create(cfg.trust.identity_path)
    trust_engine = TrustEngine(
        identity=identity,
        trust_anchors=set(cfg.trust.anchors),
        max_depth=cfg.trust.max_depth,
    )

    if cfg.storage.use_drive and cfg.storage.drive_token_path:
        from agent.drive_adapter import DriveAdapter
        storage = DriveAdapter(cfg.storage.drive_token_path)
    elif cfg.storage.use_http and cfg.storage.server_url:
        from agent.http_storage_adapter import HTTPStorageAdapter
        storage = HTTPStorageAdapter(cfg.storage.server_url)
    else:
        storage = LocalStorage(cfg.storage.root)
    
    tracker = TrackerClient(
        tracker_url=cfg.tracker.url,
        agent_id=identity.agent_id,
        host=cfg.server.host,
        port=cfg.server.port,
    )

    async with tracker:
        downloader = Downloader(
            identity=identity,
            trust_engine=trust_engine,
            tracker=tracker,
            storage=storage,
            max_peers=cfg.downloader.max_peers,
            timeout=cfg.downloader.timeout,
        )
        result = await downloader.download(file_hash, file_size)

    print(f"downloaded: {result.file_hash[:16]}...")
    print(f"bytes:      {result.total_bytes}")
    print(f"peers used: {len(result.peers_used)}")
    print(f"verified:   {result.verified}")


async def cmd_peers(
    file_hash: str,
    config_path: str = "config/settings.yaml",
) -> None:
    from agent.config import load_config
    from agent.identity import AgentIdentity
    from agent.tracker_client import TrackerClient

    cfg = load_config(config_path)
    identity = AgentIdentity.load_or_create(cfg.trust.identity_path)
    tracker = TrackerClient(
        tracker_url=cfg.tracker.url,
        agent_id=identity.agent_id,
        host=cfg.server.host,
        port=cfg.server.port,
    )
    async with tracker:
        peers = await tracker.get_peers(file_hash)

    if not peers:
        print("no peers found")
        return
    for p in peers:
        print(f"{p.agent_id[:16]}...  {p.host}:{p.port}")


async def cmd_identity(config_path: str = "config/settings.yaml") -> None:
    from agent.config import load_config
    from agent.identity import AgentIdentity

    cfg = load_config(config_path)
    identity = AgentIdentity.load_or_create(cfg.trust.identity_path)
    print(f"agent_id: {identity.agent_id}")


async def cmd_files(storage_root: str = "./data") -> None:
    from agent.storage import LocalStorage

    storage = LocalStorage(storage_root)
    files = await storage.list_files()
    if not files:
        print("no files in storage")
        return
    for f in files:
        size = await storage.get_size(f)
        print(f"{f}  ({size} bytes)")


def main() -> None:
    args = sys.argv[1:]
    if not args:
        print(__doc__)
        sys.exit(0)

    cmd = args[0]

    match cmd:
        case "seed":
            if len(args) < 2:
                print("usage: seed <file_path>", file=sys.stderr)
                sys.exit(1)
            asyncio.run(cmd_seed(args[1]))

        case "download":
            if len(args) < 3:
                print("usage: download <file_hash> <file_size>", file=sys.stderr)
                sys.exit(1)
            asyncio.run(cmd_download(args[1], int(args[2])))

        case "peers":
            if len(args) < 2:
                print("usage: peers <file_hash>", file=sys.stderr)
                sys.exit(1)
            asyncio.run(cmd_peers(args[1]))

        case "identity":
            asyncio.run(cmd_identity())

        case "files":
            asyncio.run(cmd_files())

        case _:
            print(f"unknown command: {cmd}", file=sys.stderr)
            print(__doc__)
            sys.exit(1)


if __name__ == "__main__":
    main()