"""Surgical re-fetch for the VNP46A2 ingestion-gap window (2023-2024).

Keeps the 2012-2022 + 2025-2026 portions of data/satellite/viirs_sol_monthly.csv
and regenerates only 2023-01..2024-12 rows using the updated fetch_primary
logic (which falls back to VCMSLCFG when VNP46A2 has <10 days or zero
valid pixels in a month).

Idempotent: safe to run multiple times.
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from _common import abs_path, buffers, init_ee, load_env, paths  # noqa: E402

load_env()

import importlib.util  # noqa: E402

spec = importlib.util.spec_from_file_location(
    "fvs", str(Path(__file__).parent / "fetch_viirs_sol.py")
)
fvs = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fvs)


WINDOW_START = "2023-01"
WINDOW_END = "2024-12"


def main() -> None:
    init_ee()
    p = paths()
    cfg = p["streams"]["viirs_sol"]
    cities = buffers()
    out_path = abs_path(p["data"]["viirs_sol_monthly"])

    df = pd.read_csv(out_path, parse_dates=["date"])
    keep_mask = ~df["date"].between(pd.Timestamp(WINDOW_START + "-01"),
                                     pd.Timestamp(WINDOW_END + "-01"))
    kept = df[keep_mask].copy()
    print(f"[..] keeping {len(kept)} pre-existing rows; refetching window "
          f"{WINDOW_START}..{WINDOW_END}")

    new_rows: list[dict] = []
    for city in cities:
        geom = fvs.city_geom(city)
        done = 0
        for y, m in fvs.month_iter(WINDOW_START, WINDOW_END):
            date_str = f"{y:04d}-{m:02d}-01"
            r = fvs.fetch_primary(y, m, geom, cfg) or fvs.fetch_fallback(y, m, geom, cfg)
            if r is None:
                new_rows.append({
                    "date": date_str, "city": city["name"],
                    "sol": None, "n_valid_pixels": 0, "n_total_pixels": 0,
                    "mean_rad": None, "median_rad": None, "n_masked": None,
                    "low_coverage_flag": True, "source": "missing",
                })
                continue
            n_valid = r.get("n_valid_pixels") or 0
            n_total = r.get("n_total_pixels") or 0
            low_cov = bool(n_total and (n_valid / n_total) < 0.50)
            new_rows.append({
                "date": date_str, "city": city["name"],
                "sol": r["sol"], "n_valid_pixels": n_valid,
                "n_total_pixels": n_total, "mean_rad": r["mean_rad"],
                "median_rad": r["median_rad"], "n_masked": r.get("n_masked"),
                "low_coverage_flag": low_cov, "source": r["source"],
            })
            done += 1
            time.sleep(0.03)
        print(f"[ok] {city['name']}: {done} refetched")

    new = pd.DataFrame(new_rows)
    new["date"] = pd.to_datetime(new["date"])
    # Align columns to the kept dataframe schema
    for c in kept.columns:
        if c not in new.columns:
            new[c] = pd.NA
    new = new[kept.columns]

    merged = pd.concat([kept, new], ignore_index=True).sort_values(
        ["city", "date"]).reset_index(drop=True)
    merged.to_csv(out_path, index=False)
    print(f"[ok] wrote {out_path} ({len(merged)} rows total)")


if __name__ == "__main__":
    main()
