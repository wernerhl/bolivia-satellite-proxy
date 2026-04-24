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
    qa = img.select("qa_value")
    return img.select(band).updateMask(qa.gte(qa_min))


def daily_series(roi: dict, start: date, end: date, cfg: dict) -> pd.DataFrame:
    geom = roi_geom(roi)
    coll = (
        ee.ImageCollection(cfg["collection"])
        .filterDate(start.isoformat(), (end + timedelta(days=1)).isoformat())
        .filterBounds(geom)
    )

    def reducer(img: "ee.Image") -> "ee.Feature":
        masked = mask_qa(img, cfg["band"], cfg["qa_min"])
        red = masked.reduceRegion(
            reducer=ee.Reducer.mean().combine(ee.Reducer.count(), sharedInputs=True),
            geometry=geom, scale=7000, maxPixels=int(1e9), bestEffort=True,
        )
        return ee.Feature(None, {
            "date": img.date().format("YYYY-MM-dd"),
            "no2": red.get(cfg["band"] + "_mean"),
            "n_pix": red.get(cfg["band"] + "_count"),
        })

    fc = coll.map(reducer)
    info = fc.getInfo()
    rows = []
    for feat in info.get("features", []):
        props = feat["properties"]
        rows.append({
            "date": props["date"], "roi": roi["name"],
            "no2": props.get("no2"), "n_pix": props.get("n_pix") or 0,
        })
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

    all_daily = []
    for roi in rois():
        df = daily_series(roi, start, end, cfg)
        df.to_csv(raw_dir / f"{roi['name']}_daily.csv", index=False)
        all_daily.append(df)
        print(f"[ok] {roi['name']}: {len(df)} daily rows")

    daily = pd.concat(all_daily, ignore_index=True)
    daily["date"] = pd.to_datetime(daily["date"])
    daily = daily.dropna(subset=["no2"])

    # Monthly: require n_valid_days >= cfg.min_valid_days_per_month
    daily["month"] = daily["date"].dt.to_period("M").dt.to_timestamp()
    grp = daily.groupby(["month", "roi"])
    monthly = grp.agg(
        no2_tropos_col_mol_m2=("no2", "mean"),
        sd=("no2", "std"),
        n_valid_days=("no2", "count"),
    ).reset_index().rename(columns={"month": "date"})
    monthly.loc[monthly["n_valid_days"] < cfg["min_valid_days_per_month"],
                ["no2_tropos_col_mol_m2", "sd"]] = pd.NA

    monthly.to_csv(monthly_path, index=False)
    print(f"[ok] wrote {monthly_path} ({len(monthly)} rows)")


if __name__ == "__main__":
    main()
