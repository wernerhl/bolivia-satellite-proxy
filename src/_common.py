"""Shared utilities for the Bolivia satellite-proxy pipeline.

Mirrors whl.Haiti/src/_common.py so the two projects share conventions.
GEE auth uses the Haiti service-account key; GCS outputs live under
`gs://$GCS_BUCKET/bolivia/` to keep Bolivia isolated from Haiti assets.
"""
from __future__ import annotations

import hashlib
import os
from datetime import date
from pathlib import Path

import yaml
from dotenv import load_dotenv


def project_root() -> Path:
    env_root = os.environ.get("PROJECT_ROOT")
    if env_root:
        return Path(env_root)
    return Path(__file__).resolve().parents[1]


def load_env() -> None:
    load_dotenv(project_root() / ".env")


def load_yaml(relpath: str) -> dict:
    with open(project_root() / relpath) as f:
        return yaml.safe_load(f)


def paths() -> dict:
    return load_yaml("config/paths.yaml")


def buffers() -> list[dict]:
    return load_yaml("config/buffers.yaml")["cities"]


def flares() -> dict:
    return load_yaml("config/flares.yaml")


def rois() -> list[dict]:
    return load_yaml("config/rois.yaml")["rois"]


def ensure_dir(p: str | Path) -> Path:
    q = project_root() / p if not Path(p).is_absolute() else Path(p)
    q.mkdir(parents=True, exist_ok=True)
    return q


def abs_path(relpath: str) -> Path:
    p = Path(relpath)
    return p if p.is_absolute() else project_root() / p


def require_env(var: str) -> str:
    v = os.environ.get(var)
    if not v:
        raise RuntimeError(f"Missing required env var: {var} — see .env.example")
    return v


def sha256_file(path: Path, chunk: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            b = f.read(chunk)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def init_ee() -> None:
    """Initialize Earth Engine with the Haiti service-account key.

    The GEE project is `haiti-poverty-monitoring` (reused); Bolivia exports
    live under the `bolivia/` prefix of the shared GCS bucket.
    """
    import ee
    load_env()
    project = require_env("GCP_PROJECT_ID")
    key_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if key_path and Path(key_path).exists():
        from google.oauth2 import service_account
        creds = service_account.Credentials.from_service_account_file(
            key_path,
            scopes=[
                "https://www.googleapis.com/auth/earthengine",
                "https://www.googleapis.com/auth/cloud-platform",
            ],
        )
        ee.Initialize(credentials=creds, project=project)
    else:
        ee.Initialize(project=project)


def reporting_cutoff_month(today: date | None = None) -> str:
    """Current month minus 2 (VNP46A3 publication lag). Returns 'YYYY-MM'."""
    today = today or date.today()
    y, m = today.year, today.month - 2
    if m <= 0:
        y, m = y - 1, m + 12
    return f"{y:04d}-{m:02d}"
