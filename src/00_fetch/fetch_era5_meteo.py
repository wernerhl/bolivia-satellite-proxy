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

Resilience:
  * Per-ROI partial files at data/satellite/era5_raw/{roi}.csv. The
    main loop skips ROIs whose partial file already exists.
  * GEE getInfo wrapped in retry(3) with exponential backoff to absorb
    transient HTTP 400 / "Computation timed out".
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


def _retry_get_info(thunk, retries: int = 3, base_delay: float = 5.0):
    """Call thunk() and retry on transient errors (timeouts, 400s)."""
    import time
    for i in range(retries):
        try:
            return thunk()
        except Exception as e:
            msg = str(e)
            if i == retries - 1:
                return None
            if any(k in msg for k in ("timed out", "Computation timed out",
                                       "HttpError 400", "HttpError 5",
                                       "deadline")):
                time.sleep(base_delay * (2 ** i))
                continue
            return None
    return None


def _wind_one_roi(roi: dict, start: date, end: date) -> pd.DataFrame:
    """Extract monthly u10, v10 for one ROI from ERA5_LAND/MONTHLY_AGGR."""
    geom = _roi_geom(roi)
    rows: list[dict] = []
    y, m = start.year, start.month
    while (y, m) <= (end.year, end.month):
        ny, nm = _month_end(y, m)
        coll = (ee.ImageCollection(LAND_COLL)
                .filterDate(f"{y}-{m:02d}-01", f"{ny}-{nm:02d}-01")
                .select(["u_component_of_wind_10m", "v_component_of_wind_10m"]))
        size = _retry_get_info(lambda: coll.size().getInfo())
        if not size:
            rows.append({"date": f"{y:04d}-{m:02d}-01", "roi": roi["name"],
                         "u10": None, "v10": None})
            y, m = ny, nm
            continue
        img = coll.first()
        red = _retry_get_info(lambda i=img: i.reduceRegion(
            reducer=ee.Reducer.mean(),
            geometry=geom, scale=LAND_SCALE_M, maxPixels=int(1e9),
            bestEffort=True,
        ).getInfo())
        if red is None:
            rows.append({"date": f"{y:04d}-{m:02d}-01", "roi": roi["name"],
                         "u10": None, "v10": None})
        else:
            rows.append({
                "date": f"{y:04d}-{m:02d}-01", "roi": roi["name"],
                "u10": red.get("u_component_of_wind_10m"),
                "v10": red.get("v_component_of_wind_10m"),
            })
        y, m = ny, nm
    return pd.DataFrame(rows)


def _blh_one_roi(roi: dict, start: date, end: date) -> pd.DataFrame:
    """Monthly mean BLH for one ROI via server-side reduction of ERA5/HOURLY."""
    geom = _roi_geom(roi)
    rows: list[dict] = []
    y, m = start.year, start.month
    while (y, m) <= (end.year, end.month):
        ny, nm = _month_end(y, m)
        monthly = (ee.ImageCollection(HOURLY_COLL)
                   .filterDate(f"{y}-{m:02d}-01", f"{ny}-{nm:02d}-01")
                   .select("boundary_layer_height")
                   .mean())
        red = _retry_get_info(lambda i=monthly: i.reduceRegion(
            reducer=ee.Reducer.mean(),
            geometry=geom, scale=HOURLY_SCALE_M, maxPixels=int(1e9),
            bestEffort=True,
        ).getInfo())
        blh = red.get("boundary_layer_height") if red else None
        rows.append({"date": f"{y:04d}-{m:02d}-01", "roi": roi["name"],
                     "blh": blh})
        y, m = ny, nm
    return pd.DataFrame(rows)


def main() -> None:
    init_ee()
    out_path = abs_path("data/satellite/era5_meteo_monthly.csv")
    ensure_dir(out_path.parent)
    raw_dir = ensure_dir(abs_path("data/satellite/era5_raw"))

    roi_list = rois()
    parts: list[pd.DataFrame] = []

    for i, roi in enumerate(roi_list, 1):
        per_roi = raw_dir / f"{roi['name']}.csv"
        if per_roi.exists():
            print(f"[..] {i}/{len(roi_list)} {roi['name']}: cached, skipping")
            parts.append(pd.read_csv(per_roi))
            continue
        print(f"[..] {i}/{len(roi_list)} {roi['name']}: u10/v10", flush=True)
        wind = _wind_one_roi(roi, START, END)
        print(f"[..] {i}/{len(roi_list)} {roi['name']}: blh", flush=True)
        blh = _blh_one_roi(roi, START, END)
        df = wind.merge(blh, on=["date", "roi"], how="outer")
        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values("date")[["date", "roi", "u10", "v10", "blh"]]
        df.to_csv(per_roi, index=False)
        parts.append(df)
        print(f"[ok] {i}/{len(roi_list)} {roi['name']} saved {len(df)} rows -> {per_roi.name}",
              flush=True)

    df = pd.concat(parts, ignore_index=True)
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values(["roi", "date"])[["date", "roi", "u10", "v10", "blh"]]
    df.to_csv(out_path, index=False)
    print(f"[ok] ERA5: {len(df)} rows -> {out_path}")
    print(df.groupby("roi")[["u10", "v10", "blh"]].mean().round(2))


if __name__ == "__main__":
    main()
