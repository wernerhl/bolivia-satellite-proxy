"""ERA5 monthly meteorological covariates over the 11 ROIs, 2018-07..2026-04.

Fetches three variables for each (date, roi):
  u10 — 10-m u-component of wind (m/s), monthly mean
  v10 — 10-m v-component of wind (m/s), monthly mean
  blh — boundary layer height (m), monthly mean

Source split:
  u10, v10 — `ECMWF/ERA5_LAND/MONTHLY_AGGR` (high-res ERA5-Land, native
             0.1 deg, monthly aggregate already provided)
  blh     — `ECMWF/ERA5/HOURLY` reduced server-side to monthly means
             (BLH is atmospheric-only, not in ERA5_LAND; native 0.25 deg)

Each ROI is the brief's TROPOMI rectangle (config/rois.yaml). For each
(roi, month) we extract the spatial mean over the rectangle.

Output: data/satellite/era5_meteo_monthly.csv
  Columns: date, roi, u10, v10, blh
"""
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from _common import abs_path, ensure_dir, init_ee, load_env, rois  # noqa: E402

load_env()

import ee  # noqa: E402


START = date(2018, 7, 1)
END = date(2026, 4, 30)

LAND_COLL = "ECMWF/ERA5_LAND/MONTHLY_AGGR"
HOURLY_COLL = "ECMWF/ERA5/HOURLY"

# Native scales (don't downsample beyond native — let bestEffort handle it)
LAND_SCALE_M = 11132   # ~0.1 deg at the equator
HOURLY_SCALE_M = 27830  # ~0.25 deg


def _roi_geom(roi: dict) -> "ee.Geometry":
    return ee.Geometry.Rectangle([
        min(roi["nw_lon"], roi["se_lon"]), min(roi["nw_lat"], roi["se_lat"]),
        max(roi["nw_lon"], roi["se_lon"]), max(roi["nw_lat"], roi["se_lat"]),
    ])


def _month_end(y: int, m: int) -> tuple[int, int]:
    return (y, m + 1) if m < 12 else (y + 1, 1)


def _wind_one_roi(roi: dict, start: date, end: date) -> pd.DataFrame:
    """Extract monthly u10, v10 for one ROI from ERA5_LAND/MONTHLY_AGGR.
    One server-side image per month already exists; we just reduce the
    rectangle for each."""
    geom = _roi_geom(roi)
    rows: list[dict] = []
    y, m = start.year, start.month
    while (y, m) <= (end.year, end.month):
        ny, nm = _month_end(y, m)
        coll = (ee.ImageCollection(LAND_COLL)
                .filterDate(f"{y}-{m:02d}-01", f"{ny}-{nm:02d}-01")
                .select(["u_component_of_wind_10m", "v_component_of_wind_10m"]))
        if coll.size().getInfo() == 0:
            rows.append({"date": f"{y:04d}-{m:02d}-01", "roi": roi["name"],
                         "u10": None, "v10": None})
            y, m = ny, nm
            continue
        img = coll.first()  # monthly_aggr = 1 image per month
        red = img.reduceRegion(
            reducer=ee.Reducer.mean(),
            geometry=geom, scale=LAND_SCALE_M, maxPixels=int(1e9),
            bestEffort=True,
        ).getInfo()
        rows.append({
            "date": f"{y:04d}-{m:02d}-01", "roi": roi["name"],
            "u10": red.get("u_component_of_wind_10m"),
            "v10": red.get("v_component_of_wind_10m"),
        })
        y, m = ny, nm
    return pd.DataFrame(rows)


def _blh_one_roi(roi: dict, start: date, end: date) -> pd.DataFrame:
    """Extract monthly mean BLH for one ROI from ERA5/HOURLY by
    server-side reducing the hourly collection to month-means before
    sampling. One reduceRegion per (roi, month)."""
    geom = _roi_geom(roi)
    rows: list[dict] = []
    y, m = start.year, start.month
    while (y, m) <= (end.year, end.month):
        ny, nm = _month_end(y, m)
        monthly = (ee.ImageCollection(HOURLY_COLL)
                   .filterDate(f"{y}-{m:02d}-01", f"{ny}-{nm:02d}-01")
                   .select("boundary_layer_height")
                   .mean())
        try:
            red = monthly.reduceRegion(
                reducer=ee.Reducer.mean(),
                geometry=geom, scale=HOURLY_SCALE_M, maxPixels=int(1e9),
                bestEffort=True,
            ).getInfo()
            blh = red.get("boundary_layer_height")
        except Exception:
            blh = None
        rows.append({"date": f"{y:04d}-{m:02d}-01", "roi": roi["name"],
                     "blh": blh})
        y, m = ny, nm
    return pd.DataFrame(rows)


def main() -> None:
    init_ee()
    out_path = abs_path("data/satellite/era5_meteo_monthly.csv")
    ensure_dir(out_path.parent)

    roi_list = rois()
    wind_parts: list[pd.DataFrame] = []
    blh_parts: list[pd.DataFrame] = []

    for i, roi in enumerate(roi_list, 1):
        print(f"[..] {i}/{len(roi_list)} {roi['name']}: u10/v10")
        wind_parts.append(_wind_one_roi(roi, START, END))
        print(f"[..] {i}/{len(roi_list)} {roi['name']}: blh")
        blh_parts.append(_blh_one_roi(roi, START, END))

    wind = pd.concat(wind_parts, ignore_index=True)
    blh = pd.concat(blh_parts, ignore_index=True)
    df = wind.merge(blh, on=["date", "roi"], how="outer")
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values(["roi", "date"])[["date", "roi", "u10", "v10", "blh"]]
    df.to_csv(out_path, index=False)
    print(f"[ok] ERA5: {len(df)} rows -> {out_path}")
    print(df.groupby("roi")[["u10", "v10", "blh"]].mean().round(2))


if __name__ == "__main__":
    main()
