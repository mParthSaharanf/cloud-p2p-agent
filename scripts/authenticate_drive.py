#!/usr/bin/env python3
"""
One-time OAuth flow to authorize a Google account for Drive access.
Run this once per agent to generate a token file.

Usage:
    python scripts/authenticate_drive.py --token config/token_agent_a.json
    python scripts/authenticate_drive.py --token config/token_agent_b.json
"""
import argparse
import json
from pathlib import Path

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES = ["https://www.googleapis.com/auth/drive.file"]
CREDENTIALS_FILE = "config/drive_credentials.json"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--token", required=True, help="Where to save the token")
    args = parser.parse_args()

    flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
    creds = flow.run_local_server(port=0)

    token_path = Path(args.token)
    token_path.parent.mkdir(parents=True, exist_ok=True)
    token_path.write_text(creds.to_json())
    token_path.chmod(0o600)

    print(f"token saved to {args.token}")
    print(f"authorized as: {creds.token}")


if __name__ == "__main__":
    main()