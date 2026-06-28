# agent/config.py
from __future__ import annotations

import os
from pathlib import Path

import yaml
from pydantic import BaseModel, Field


class TrackerConfig(BaseModel):
    url: str = "http://localhost:8000"


class ServerConfig(BaseModel):
    host: str = "0.0.0.0"
    port: int = 9000


class StorageConfig(BaseModel):
    root: str = "./data"
    use_http: bool = False
    server_url: str = ""
    use_drive: bool = False
    drive_token_path: str = ""


class TrustConfig(BaseModel):
    max_depth: int = 3
    identity_path: str = "config/identity.pem"
    # Comma-separated base64 agent_ids to treat as trust anchors
    # beyond self. Empty by default — agent trusts only itself until
    # a vouch chain is established.
    anchors: list[str] = Field(default_factory=list)


class DownloaderConfig(BaseModel):
    max_peers: int = 4
    timeout: float = 30.0


class AgentConfig(BaseModel):
    tracker: TrackerConfig = Field(default_factory=TrackerConfig)
    server: ServerConfig = Field(default_factory=ServerConfig)
    storage: StorageConfig = Field(default_factory=StorageConfig)
    trust: TrustConfig = Field(default_factory=TrustConfig)
    downloader: DownloaderConfig = Field(default_factory=DownloaderConfig)


def load_config(path: str | Path = "config/settings.yaml") -> AgentConfig:
    """
    Load config from a YAML file, falling back to defaults for any
    missing keys. Environment variables override file values:
        P2P_TRACKER_URL, P2P_SERVER_PORT, P2P_STORAGE_ROOT, etc.
    This lets Docker deployments configure via env without needing to
    mount a settings.yaml file.
    """
    path = Path(path)
    data: dict = {}

    if path.exists():
        with path.open() as f:
            data = yaml.safe_load(f) or {}

    config = AgentConfig.model_validate(data)

    # Environment variable overrides
    if url := os.environ.get("P2P_TRACKER_URL"):
        config.tracker.url = url
    if port := os.environ.get("P2P_SERVER_PORT"):
        config.server.port = int(port)
    if root := os.environ.get("P2P_STORAGE_ROOT"):
        config.storage.root = root
    if depth := os.environ.get("P2P_TRUST_MAX_DEPTH"):
        config.trust.max_depth = int(depth)
    if host := os.environ.get("P2P_ADVERTISE_HOST"):
        config.server.host = host
    if os.environ.get("P2P_USE_HTTP_STORAGE", "").lower() == "true":
        config.storage.use_http = True
    if url := os.environ.get("P2P_STORAGE_SERVER_URL"):
        config.storage.server_url = url
    if os.environ.get("P2P_USE_DRIVE", "").lower() == "true":
        config.storage.use_drive = True
    if token := os.environ.get("P2P_DRIVE_TOKEN_PATH"):
        config.storage.drive_token_path = token
        
    return config