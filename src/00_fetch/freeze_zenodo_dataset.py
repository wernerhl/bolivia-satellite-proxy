"""Track F — freeze the replication dataset for Zenodo upload.

Bundles every canonical satellite and official series into Parquet under
outputs/zenodo/, plus a DATASET_VINTAGE.json manifest with vintage
timestamp, git hash, row counts, and file hashes. Does not upload; run
`zenodo_get`-style push separately once the DOI is reserved.

The submission workflow:
  1. make all                       # complete pipeline on current vintage
  2. make freeze-zenodo             # this script
  3. zenodo-cli upload outputs/zenodo/  # reserve DOI, paste into paper
"""
from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from _common import abs_path, ensure_dir, load_env, paths, sha256_file  # noqa: E402

load_env()


CANONICAL_SATELLITE = [
    "viirs_sol_monthly",
    "viirs_sol_anomaly",
    "vnf_monthly",
    "vnf_anomaly",
    "s5p_monthly",
    "s5p_anomaly",
    "s2_ndvi_monthly",
    "s2_ndvi_anomaly",
    "ci",
]
CANONICAL_OFFICIAL = [
    "official_igae",
    "official_cement",
    "official_cndc",
    "official_ypfb",
    "official_sin",
    "official_aduana",
    "wb_ggfr_country",
    "wb_ggfr_flares",
]
CANONICAL_RESULTS = [
    "data/satellite/dfm_result.json",
    "data/satellite/dfm_twofactor_result.json",
    "data/satellite/recession_dating.json",
    "data/satellite/manipulation_tests.json",
    "data/satellite/vnf_calibration_field.json",
    "data/satellite/vnf_wb_crosscheck.json",
    "data/satellite/benchmark_ine.json",
    "data/satellite/elasticity_viirs.json",
    "data/satellite/elasticity_vnf.json",
    "data/satellite/elasticity_no2.json",
]


def _git_info() -> dict:
    def _run(cmd: list[str]) -> str | None:
        try:
            return subprocess.check_output(cmd, text=True).strip()
        except Exception:
            return None
    return {
        "commit": _run(["git", "rev-parse", "HEAD"]),
        "branch": _run(["git", "rev-parse", "--abbrev-ref", "HEAD"]),
        "dirty": bool(_run(["git", "status", "--porcelain"])),
    }


def _csv_to_parquet(src: Path, dst: Path) -> dict | None:
    if not src.exists():
        return None
    df = pd.read_csv(src, low_memory=False)
    dst.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(dst, index=False)
    return {
        "source_csv": str(src.relative_to(abs_path("."))),
        "parquet": str(dst.relative_to(abs_path("."))),
        "rows": int(len(df)),
        "cols": int(len(df.columns)),
        "sha256": sha256_file(dst),
    }


def main() -> None:
    p = paths()
    out_dir = ensure_dir(abs_path("outputs/zenodo"))

    manifest: dict = {
        "vintage": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "project": p["project"]["name"],
        "git": _git_info(),
        "series": {},
        "results_json": {},
        "citation": {
            "author": "Hernani-Limarino, Werner",
            "title": "Fires, Lights, and Smog: Replication dataset",
            "version": "v2-draft",
            "type": "dataset",
        },
    }

    for key in CANONICAL_SATELLITE + CANONICAL_OFFICIAL:
        src = abs_path(p["data"].get(key, ""))
        if not src or not src.exists():
            manifest["series"][key] = {"status": "not_fetched"}
            continue
        dst = out_dir / f"{key}.parquet"
        info = _csv_to_parquet(src, dst)
        manifest["series"][key] = info or {"status": "read_failed"}

    for rpath in CANONICAL_RESULTS:
        src = abs_path(rpath)
        if not src.exists():
            manifest["results_json"][rpath] = {"status": "not_fetched"}
            continue
        dst = out_dir / Path(rpath).name
        dst.write_text(src.read_text())
        manifest["results_json"][rpath] = {
            "copied_to": str(dst.relative_to(abs_path("."))),
            "sha256": sha256_file(dst),
        }

    manifest_path = out_dir / "DATASET_VINTAGE.json"
    manifest_path.write_text(json.dumps(manifest, indent=2))

    n_ok = sum(1 for v in manifest["series"].values() if "rows" in v)
    print(f"[ok] Zenodo bundle → {out_dir}")
    print(f"  {n_ok} / {len(manifest['series'])} canonical series archived")
    print(f"  manifest: {manifest_path.relative_to(abs_path('.'))}")


if __name__ == "__main__":
    main()
