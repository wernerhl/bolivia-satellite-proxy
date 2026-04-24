"""Refresh the EOG VNF bearer token via Keycloak password grant.

Reads EOG_USER and EOG_PASS from .env; writes the new access_token back
into .env under EOG_TOKEN. Intended to run on a 60-day cadence (cron or
ScheduleWakeup) roughly 48h before the previous token expires.

No response body is logged. On failure exits non-zero for the harness.
"""
from __future__ import annotations

import os
import re
import sys
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from _common import load_env, project_root, require_env  # noqa: E402

load_env()


TOKEN_URL = "https://eogauth.mines.edu/auth/realms/master/protocol/openid-connect/token"
# EOG's public OIDC client; this secret is documented in their own
# download examples and is not sensitive.
CLIENT_ID = "eogdata_oidc"
CLIENT_SECRET = "2677ad81-521b-4869-8480-6d05b9e57d48"


def fetch_token(user: str, password: str) -> str:
    r = requests.post(
        TOKEN_URL,
        data={
            "username": user, "password": password,
            "grant_type": "password",
            "client_id": CLIENT_ID, "client_secret": CLIENT_SECRET,
        },
        timeout=30,
    )
    r.raise_for_status()
    return r.json()["access_token"]


def write_env_token(token: str) -> Path:
    env = project_root() / ".env"
    text = env.read_text() if env.exists() else ""
    if re.search(r"^EOG_TOKEN=.*$", text, flags=re.M):
        new = re.sub(r"^EOG_TOKEN=.*$", f"EOG_TOKEN={token}", text, flags=re.M)
    else:
        new = text + ("\n" if text and not text.endswith("\n") else "") + f"EOG_TOKEN={token}\n"
    env.write_text(new)
    return env


def main() -> None:
    user = os.environ.get("EOG_USER") or require_env("EOG_USER")
    pw = os.environ.get("EOG_PASS") or require_env("EOG_PASS")
    token = fetch_token(user, pw)
    path = write_env_token(token)
    print(f"[ok] wrote fresh EOG_TOKEN → {path}")


if __name__ == "__main__":
    main()
