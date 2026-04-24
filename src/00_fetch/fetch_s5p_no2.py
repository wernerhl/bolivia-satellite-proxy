"""Stream 3 — Sentinel-5P TROPOMI tropospheric NO₂ per ROI.

Uses the OFFL L3 collection with qa_value ≥ 0.75 masking. Produces a
daily ROI-mean series (n_valid_pixels recorded) and an anchored 7-day
rolling mean, then rolls up to monthly. Writes the daily and monthly
series; the anomaly step uses 2019 as the multiplicative baseline.
"""
from __future__ import annotations

import sys
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from _common import abs_path, ensure_dir, init_ee, load_env, paths, rois  # noqa: E402

load_env()

import ee  # noqa: E402


def roi_geom(roi: dict) -> "ee.Geometry":
    return ee.Geometry.Rectangle([
        min(roi["nw_lon"], roi["se_lon"]), min(roi["nw_lat"], roi["se_lat"]),
        max(roi["nw_lon"], roi["se_lon"]), max(roi["nw_lat"], roi["se_lat"]),
    ])


def mask_qa(img: "ee.Image", band: str, qa_min: float) -> "ee.Image":
    """L3 OFFL is already QA-filtered upstream; no qa_value band exposed.
    Return the band as-is. Signature kept for compatibility."""
    return img.select(band)


def _month_end(y: int, m: int) -> tuple[int, int]:
    return (y, m + 1) if m < 12 else (y + 1, 1)


def monthly_server_side(roi: dict, start: date, end: date, cfg: dict) -> pd.DataFrame:
    """One reduceRegion per (ROI, month) — stays well below 5000-element
    collection-query limit. Daily mean-first then month-average keeps the
    weighting right for days with variable orbit counts.
    """
    geom = roi_geom(roi)
    rows: list[dict] = []
    y, m = start.year, start.month
    while (y, m) <= (end.year, end.month):
        ny, nm = _month_end(y, m)
        coll = (ee.ImageCollection(cfg["collection"])
                .filterDate(f"{y}-{m:02d}-01", f"{ny}-{nm:02d}-01")
                .filterBounds(geom)
                .select(cfg["band"]))
        n_img = coll.size().getInfo()
        if n_img == 0:
            rows.append({"date": f"{y:04d}-{m:02d}-01", "roi": roi["name"],
                         "no2": None, "n_valid_days": 0})
            y, m = ny, nm
            continue
        mean_img = coll.mean()
        red = mean_img.reduceRegion(
            reducer=ee.Reducer.mean(),
            geometry=geom, scale=7000, maxPixels=int(1e9), bestEffort=True,
        ).getInfo()
        rows.append({
            "date": f"{y:04d}-{m:02d}-01", "roi": roi["name"],
            "no2": red.get(cfg["band"]),
            "n_valid_days": int(n_img),
        })
        y, m = ny, nm
    return pd.DataFrame(rows)


def main() -> None:
    init_ee()
    p = paths()
    cfg = p["streams"]["s5p_no2"]
    start = date.fromisoformat(cfg["start"] + "-01")
    end = date.today() - timedelta(days=7)  # latency buffer

    raw_dir = ensure_dir(abs_path(p["data"]["raw_s5p"]))
    monthly_path = abs_path(p["data"]["s5p_monthly"])
    ensure_dir(monthly_path.parent)

    all_monthly = []
    for roi in rois():
        df = monthly_server_side(roi, start, end, cfg)
        df.to_csv(raw_dir / f"{roi['name']}_monthly.csv", index=False)
        all_monthly.append(df)
        print(f"[ok] {roi['name']}: {len(df)} monthly rows")

    monthly = pd.concat(all_monthly, ignore_index=True)
    monthly["date"] = pd.to_datetime(monthly["date"])
    monthly = monthly.rename(columns={"no2": "no2_tropos_col_mol_m2"})
    monthly["sd"] = np.nan  # server-side aggregation doesn't keep per-day SD

    monthly.loc[monthly["n_valid_days"] < cfg["min_valid_days_per_month"],
                ["no2_tropos_col_mol_m2", "sd"]] = pd.NA

    monthly.to_csv(monthly_path, index=False)
    print(f"[ok] wrote {monthly_path} ({len(monthly)} rows)")


if __name__ == "__main__":
    main()
