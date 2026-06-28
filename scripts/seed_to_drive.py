#!/usr/bin/env python3
"""
Seed a file into an agent's Google Drive storage.
Usage:
    python scripts/seed_to_drive.py --file /path/to/file --token config/token_agent_a.json
"""
import argparse
import asyncio
import hashlib
from pathlib import Path
import sys
import os

sys.path.insert(0, os.getcwd())

from agent.drive_adapter import DriveAdapter


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", required=True)
    parser.add_argument("--token", required=True)
    args = parser.parse_args()

    path = Path(args.file)
    if not path.exists():
        print(f"error: file not found: {args.file}")
        sys.exit(1)

    content = path.read_bytes()
    file_hash = hashlib.sha256(content).hexdigest()

    print(f"hash: {file_hash}")
    print(f"size: {len(content)} bytes")
    print("uploading to Google Drive...")

    adapter = DriveAdapter(token_path=args.token)

    async def stream():
        yield content

    total = await adapter.write_stream(file_hash, stream())
    print(f"uploaded: {total} bytes")
    print(f"drive file id: {adapter._manifest[file_hash]}")


if __name__ == "__main__":
    asyncio.run(main())