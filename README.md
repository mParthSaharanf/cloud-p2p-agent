# Cloud P2P Agent Network

A decentralized, stateless peer-to-peer file transfer network where agents run in cloud containers and transfer files directly between cloud storage backends — without exposing home IP addresses or consuming home bandwidth.

## The Problem

Traditional BitTorrent has three real bottlenecks:

1. Requires home hardware to stay powered on and connected indefinitely
2. Exposes the user's home public IP to the swarm
3. Saturates home bandwidth while seeding

Meanwhile, people have large amounts of idle storage in personal cloud drives (Google Drive, Dropbox) with no protocol to transfer data directly between them at data-center speeds.

## The Solution

Each user deploys a lightweight, stateless Agent into a cloud container. Agents form a P2P swarm and transfer files directly between cloud storage backends, bypassing home connections entirely in production deployments.

```
[Google Drive / Cloud Storage A]     [Google Drive / Cloud Storage B]
              |                                      |
          [Agent A]  <---- direct chunk stream ---> [Agent B]
              |                                      |
          [Tracker]  <---- discovery only ---------->
```

## Architecture

**Wire Protocol** — messages.py, serializer.py

Typed Pydantic v2 message models with manual dispatch serialization. Control plane (JSON) is strictly separated from data plane (raw bytes). Chunk data never touches a JSON parser, preserving streaming throughput.

**Cryptographic Identity** — identity.py

Ed25519 keypairs, persistent across restarts. Every agent has a stable agent_id (base64-encoded 32-byte public key) used as its canonical identity throughout the protocol. Private keys stored with 0o600 permissions.

**Trust Chain Engine** — trust.py

Vouch-chain-walking with configurable max depth, expiry, and revocation. Similar to X.509 certificate chains. An agent is trusted only if a signed chain of vouches leads back to a known trust anchor. Each verification returns a TrustResult carrying failure reason, depth, and trust status.

**Zero-Disk Streaming** — storage.py, http_storage_adapter.py, drive_adapter.py

Abstract StorageBackend interface decouples where bytes live from how agents move bytes. Bytes flow through async generators in 64KB chunks, never fully buffered to disk regardless of file size. The same agent logic works with local disk (dev), a mock HTTP file server (integration tests), or Google Drive (production) by swapping one env var.

**Swarm Splitting** — downloader.py

A downloading agent opens concurrent connections to multiple seeding peers, pulling disjoint byte ranges in parallel via asyncio.gather. Ranges are split proportionally across available trusted peers, reassembled in order, and verified with a final sha256 hash before being written to the destination storage backend.

**Centralized Discovery, Decentralized Transfer** — tracker/

A lightweight FastAPI tracker handles only peer discovery. TTL-based peer expiry prunes stale records on read. Once two agents know each other, all file streaming happens directly peer-to-peer, bypassing the tracker entirely. Redis-ready data structure for production scaling.

## Stack

- Python 3.12+ — asyncio, async generators, type hints throughout
- FastAPI + Uvicorn — tracker and p2p server
- httpx — async HTTP client for tracker, storage APIs, peer connections
- Pydantic v2 — wire protocol message validation
- cryptography — Ed25519 keypair generation, signing, verification
- Google Drive API — production cloud storage backend
- Podman / Docker + Compose — multi-agent local demo swarm

## Quick Start

Prerequisites: Docker or Podman with compose plugin, Python 3.12+

Clone and install:

```
git clone https://github.com/mParthSaharanf/cloud-p2p-agent
cd cloud-p2p-agent
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
```

Run the demo swarm with mock cloud storage:

```
docker compose build
./scripts/demo.sh
```

This spins up 1 tracker + 1 mock cloud file server + 3 agents. agent_b and agent_c download a 2MB file concurrently from agent_a, which streams it from the mock cloud storage backend.

Expected output:

```
downloaded: eae57307e09d72dc...
bytes:      2097152
peers used: 1
verified:   True

downloaded: eae57307e09d72dc...
bytes:      2097152
peers used: 1
verified:   True
```

## Run with Real Google Drive

1. Create a Google Cloud project, enable Drive API, create OAuth2 credentials
2. Save credentials as config/drive_credentials.json
3. Authenticate two Google accounts:

```
python scripts/authenticate_drive.py --token config/token_agent_a.json
python scripts/authenticate_drive.py --token config/token_agent_b.json
```

4. Seed a file to agent_a's Drive:

```
python scripts/seed_to_drive.py --file /path/to/file --token config/token_agent_a.json
```

5. Start the swarm and download from agent_b:

```
docker compose up -d
docker compose exec -e P2P_USE_DRIVE=true -e P2P_DRIVE_TOKEN_PATH=/config/token_agent_b.json agent_b bash -c 'python -m agent.cli download <hash> <size>'
```

The file transfers from Google Drive Account A to Google Drive Account B through the agent network. Local disk is never touched.

## Project Structure

```
cloud-p2p-agent/
├── tracker/
│   ├── main.py                   # FastAPI tracker — peer discovery
│   └── registry.py               # In-memory peer store, TTL expiry, Redis-ready
├── agent/
│   ├── messages.py               # Wire protocol message types
│   ├── serializer.py             # Encode/decode + dispatch
│   ├── identity.py               # Ed25519 keypair, sign/verify
│   ├── trust.py                  # Vouch chain engine
│   ├── storage.py                # Abstract StorageBackend + LocalStorage
│   ├── http_storage_adapter.py   # Mock cloud storage via Range requests
│   ├── drive_adapter.py          # Google Drive backend
│   ├── tracker_client.py         # Agent-side tracker HTTP client
│   ├── p2p_server.py             # Seeding side — handshake + chunk streaming
│   ├── downloader.py             # Leeching side — swarm splitting + assembly
│   ├── config.py                 # Config schema + env var overrides
│   ├── main.py                   # Agent daemon entry point
│   └── cli.py                    # Manual commands
├── fileserver/
│   └── main.py                   # Mock cloud storage server for dev/demo
├── tests/                        # 97 passing tests
├── scripts/
│   ├── demo.sh                   # One-command demo
│   ├── authenticate_drive.py     # OAuth flow for Google accounts
│   └── seed_to_drive.py          # Upload a file to Drive for seeding
├── Dockerfile
├── Dockerfile.fileserver
└── docker-compose.yml
```

## Test Suite

```
pytest tests/ -v
```

97 passed across: messages, serializer, identity, trust, storage, tracker, tracker client, p2p server, downloader.

## Production Considerations

What the demo proves:
- End-to-end file transfer between two cloud storage accounts
- Cryptographic peer identity and trust verification
- Concurrent multi-peer swarm splitting
- Zero local disk involvement

What a production version would add:
- Single-pass streaming upload to Drive using google-resumable-media library (current implementation buffers chunks in memory)
- Passphrase-encrypted private key storage
- Full vouch chain passing on every transfer request (currently bypassed with P2P_TRUST_ALL=true in dev)
- Redis-backed tracker registry for horizontal scaling
- Dropbox and S3 storage backends via the same StorageBackend interface
- Rate limiting and exponential backoff for Drive API quota
- Agent-to-agent TLS for encrypted transport
