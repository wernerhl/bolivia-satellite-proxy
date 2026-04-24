"""Stream 2 — EOG VIIRS Nightfire (VNF) daily fetch for the Chaco bbox.

Persists raw detections as JSONL per day under data/satellite/vnf_raw/YYYY-MM-DD.jsonl.
Idempotent: skips days already on disk. Retries 5xx/429 with exponential backoff.

EOG exposes VNF as daily global files; we download the nightly product and
filter to the Chaco bbox locally. For the production deployment, replace the
query URL with the bbox-query endpoint once EOG exposes one.
"""
from __future__ import annotations

import gzip
import io
import json
import sys
import time
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from _common import abs_path, ensure_dir, load_env, paths, require_env  # noqa: E402

load_env()


VNF_LIST_URL = "https://eogdata.mines.edu/api/v1/vnf"


def in_bbox(lat: float, lon: float, bbox: list[float]) -> bool:
    return (bbox[0] <= lon <= bbox[2]) and (bbox[1] <= lat <= bbox[3])


def _get(url: str, token: str, stream: bool = False) -> requests.Response:
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    delay = 5
    for attempt in range(6):
        r = requests.get(url, headers=headers, stream=stream, timeout=120)
        if r.status_code < 400:
            return r
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(delay)
            delay = min(delay * 2, 4 * 3600)
            continue
        r.raise_for_status()
    raise RuntimeError(f"EOG VNF fetch failed after retries: {url}")


def list_day_products(day: date, token: str) -> list[dict]:
    """Return metadata rows for all VNF files covering `day`. The EOG API
    structure is stable at the version-prefix level; product-file links are
    inside each record."""
    url = f"{VNF_LIST_URL}?date_start={day.isoformat()}&date_end={day.isoformat()}"
    r = _get(url, token)
    try:
        return r.json().get("data", [])
    except Exception:
        return []


def parse_csv_gz(url: str, token: str, bbox: list[float]) -> list[dict]:
    r = _get(url, token, stream=True)
    buf = io.BytesIO(r.content)
    with gzip.open(buf, "rt") as f:
        df = pd.read_csv(f, low_memory=False)
    lat_col = next((c for c in df.columns if c.lower() in ("lat", "lat_gmtco")), None)
    lon_col = next((c for c in df.columns if c.lower() in ("lon", "lon_gmtco")), None)
    if lat_col is None or lon_col is None:
        return []
    df = df[(df[lat_col].between(bbox[1], bbox[3]))
            & (df[lon_col].between(bbox[0], bbox[2]))]
    return df.to_dict(orient="records")


def fetch_day(day: date, token: str, bbox: list[float], out_dir: Path) -> Path | None:
    out_path = out_dir / f"{day.isoformat()}.jsonl"
    if out_path.exists():
        return out_path
    products = list_day_products(day, token)
    records: list[dict] = []
    for prod in products:
        url = prod.get("productUrl") or prod.get("url") or ""
        if not url.endswith(".csv.gz"):
            continue
        try:
            records.extend(parse_csv_gz(url, token, bbox))
        except Exception as e:
            print(f"[warn] {day} product fetch failed: {e}")
    if not records:
        out_path.write_text("")
        return out_path
    with open(out_path, "w") as f:
        for rec in records:
            f.write(json.dumps(rec) + "\n")
    return out_path


def main() -> None:
    p = paths()
    cfg = p["streams"]["vnf"]
    token = require_env("EOG_TOKEN")

    out_dir = ensure_dir(abs_path(p["data"]["raw_vnf"]))
    start = date.fromisoformat(cfg["start"])
    end = date.today() - timedelta(days=2)  # 24-48h latency

    d = start
    n_new = 0
    while d <= end:
        out_path = out_dir / f"{d.isoformat()}.jsonl"
        if not out_path.exists():
            fetch_day(d, token, cfg["bbox"], out_dir)
            n_new += 1
            if n_new % 30 == 0:
                print(f"[..] fetched through {d}")
            time.sleep(0.1)
        d += timedelta(days=1)
    print(f"[ok] VNF raw up to {end}; {n_new} new days written to {out_dir}")


if __name__ == "__main__":
    main()
