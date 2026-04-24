"""Export a clean parquet for the La Linterna / whl.EarlyWarning DFM module.

The DFM consumes this satellite CI as one of ~8–12 coincident series.
Downstream contract:
  * monthly frequency, month-start date
  * columns: date, viirs_z, vnf_z, no2_z, ci
  * all columns standardized (z-score), missing as NaN
  * no forward fill; DFM handles ragged-edge

Writes to outputs/dfm/bolivia_satellite_ci.parquet. A tiny JSON manifest
captures the run timestamp, column dictionary, and vintage — the DFM
reader uses the manifest to detect revisions.
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from _common import abs_path, ensure_dir, load_env, paths  # noqa: E402

load_env()


SCHEMA = {
    "date": "month-start timestamp, UTC",
    "viirs_z": "population-weighted mean of per-city log-SOL z-scores (11 city buffers)",
    "vnf_z": "z-score of Chaco-total log(ΣRH) from VIIRS Nightfire",
    "no2_z": "simple mean of La Paz–El Alto and Santa Cruz TROPOMI NO2 z-scores",
    "ci": "0.40·viirs_z + 0.30·vnf_z + 0.30·no2_z; NaN if all three missing",
}


def main() -> None:
    p = paths()
    ci = pd.read_csv(abs_path(p["data"]["ci"]), parse_dates=["date"])
    ci = ci.sort_values("date").reset_index(drop=True)

    out_dir = ensure_dir("outputs/dfm")
    parquet_path = out_dir / "bolivia_satellite_ci.parquet"
    ci.to_parquet(parquet_path, index=False)

    manifest = {
        "vintage": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "n_rows": int(len(ci)),
        "date_min": str(ci["date"].min()) if not ci.empty else None,
        "date_max": str(ci["date"].max()) if not ci.empty else None,
        "columns": SCHEMA,
        "aggregation_weights": {"viirs": 0.40, "vnf": 0.30, "no2": 0.30},
        "consumer": "whl.EarlyWarning / whl.LaLinterna DFM module",
        "source_spec": "bolivia_satellite_proxy_agent_prompt.md",
    }
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2))
    print(f"[ok] DFM export → {parquet_path}  (n={len(ci)})")


if __name__ == "__main__":
    main()
