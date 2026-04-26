"""OMI/Aura tropospheric NO2 column, daily Level-3 OMNO2d, 2004-10..2018-06.

Source: AWS Open Data registry, public S3 bucket `omi-no2-nasa`
(us-west-2 region, no AWS account required). Files are NASA's
Cloud-Optimized GeoTIFF re-projection of the OMNO2d HDF-EOS5 archive.
The single band is `ColumnAmountNO2TropCloudScreened`, already
filtered upstream at CloudFraction < 30 percent — exactly what the
brief specifies.

Filename pattern (flat prefix):
  OMI-Aura_L3-OMNO2d_YYYYmMMDD_v003-{processing_stamp}.tif

Each file is a 1440 × 720 grid at 0.25° × 0.25° in EPSG:4326,
nodata = -1.27e30, units molec/cm².

Approach:
  1. List all *.tif under s3://omi-no2-nasa/ in [START, END]
  2. For each ROI, compute the integer pixel window once
  3. For each daily file: open via /vsis3/, windowed read only for the
     three ROI windows. About a few hundred bytes per ROI per day
     thanks to the COG layout — total bandwidth is tens of MB, not
     gigabytes.
  4. Daily mean per ROI → monthly mean per ROI, requiring n_valid_days
     ≥ 15 per the brief.
  5. Convert molec/cm² to mol/m² for cross-comparability with the
     TROPOMI series (1 molec/cm² = 1e4 molec/m²; molec → mol via
     Avogadro).

Output schema (per brief):
  date, roi, no2_tropos_col_mol_m2, n_valid_days, sensor
"""
from __future__ import annotations

import os
import re
import sys
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from _common import abs_path, ensure_dir, load_env, rois  # noqa: E402

load_env()

os.environ.setdefault("AWS_NO_SIGN_REQUEST", "YES")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-west-2")

import rasterio  # noqa: E402
from rasterio.windows import Window  # noqa: E402


BUCKET = "omi-no2-nasa"
S3_PREFIX = f"s3://{BUCKET}/"
LON_RES = 0.25
LAT_RES = 0.25
N_LON = 1440
N_LAT = 720
NODATA = -1.2676506e30
AVOGADRO = 6.02214076e23

START = date(2004, 10, 1)   # OMI/Aura L3 OMNO2d archive begins here
END = date(2018, 6, 30)
ROI_NAMES = {"la_paz_el_alto", "santa_cruz", "cochabamba"}

FILENAME_RE = re.compile(
    r"OMI-Aura_L3-OMNO2d_(\d{4})m(\d{2})(\d{2})_v003-[\dmt]+\.tif$")


def _list_keys() -> list[tuple[date, str]]:
    """Stream-list every OMNO2d daily filename in the bucket; return
    (date, full s3 url) tuples. Uses boto3 with anonymous config."""
    import boto3
    from botocore import UNSIGNED
    from botocore.config import Config
    s3 = boto3.client("s3",
                      region_name="us-west-2",
                      config=Config(signature_version=UNSIGNED))
    paginator = s3.get_paginator("list_objects_v2")
    out: list[tuple[date, str]] = []
    for page in paginator.paginate(Bucket=BUCKET):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            m = FILENAME_RE.match(key.rsplit("/", 1)[-1])
            if not m:
                continue
            try:
                d = date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
            except ValueError:
                continue
            if START <= d <= END:
                out.append((d, f"{S3_PREFIX}{key}"))
    out.sort(key=lambda t: t[0])
    return out


def _roi_window(roi: dict) -> tuple[Window, int]:
    """Convert NW/SE corners (deg) to a rasterio Window in the OMNO2d
    grid. Returns (window, n_native_pixels)."""
    lon_min = min(roi["nw_lon"], roi["se_lon"])
    lon_max = max(roi["nw_lon"], roi["se_lon"])
    lat_min = min(roi["nw_lat"], roi["se_lat"])
    lat_max = max(roi["nw_lat"], roi["se_lat"])
    # Grid is row 0 = lat 90, col 0 = lon -180.
    col0 = max(0, int((lon_min + 180) / LON_RES))
    col1 = min(N_LON, int(np.ceil((lon_max + 180) / LON_RES)))
    row0 = max(0, int((90 - lat_max) / LAT_RES))
    row1 = min(N_LAT, int(np.ceil((90 - lat_min) / LAT_RES)))
    if col1 == col0:
        col1 = col0 + 1
    if row1 == row0:
        row1 = row0 + 1
    win = Window(col0, row0, col1 - col0, row1 - row0)
    return win, (col1 - col0) * (row1 - row0)


def _read_roi_mean(url: str, win: Window) -> float | None:
    """Open a single OMNO2d COG over S3 and read just the ROI window.
    Returns the spatial mean of valid (non-fill) pixels in molec/cm², or
    None if no valid pixels."""
    try:
        with rasterio.open(url) as src:
            arr = src.read(1, window=win, masked=False)
    except Exception:
        return None
    valid = arr[(arr != NODATA) & np.isfinite(arr) & (arr > 0)]
    if valid.size == 0:
        return None
    return float(valid.mean())


def _read_all_rois(url: str, windows: dict[str, tuple[Window, int]]
                   ) -> dict[str, float | None]:
    """Open the COG once and read all ROI windows. Cuts S3 round-trips
    by 3x compared to opening per ROI."""
    out: dict[str, float | None] = {}
    try:
        with rasterio.open(url) as src:
            for name, (win, _n) in windows.items():
                arr = src.read(1, window=win, masked=False)
                valid = arr[(arr != NODATA) & np.isfinite(arr) & (arr > 0)]
                out[name] = float(valid.mean()) if valid.size else None
    except Exception as e:
        for name in windows:
            out[name] = None
    return out


def main() -> None:
    out_path = abs_path("data/satellite/no2_omi_monthly.csv")
    ensure_dir(out_path.parent)

    selected = [r for r in rois() if r["name"] in ROI_NAMES]
    windows = {r["name"]: _roi_window(r) for r in selected}

    print("[..] listing OMI files in S3...")
    keys = _list_keys()
    print(f"[ok] {len(keys)} daily OMI files in [{START}, {END}]")

    # Accumulate per (roi, year-month)
    accum: dict[tuple, dict] = {}
    last_year = None
    for i, (d, url) in enumerate(keys):
        if d.year != last_year:
            yr_count = sum(1 for k in keys if k[0].year == d.year)
            print(f"[..] {d.year}: {yr_count} files")
            last_year = d.year
        roi_means = _read_all_rois(url, windows)
        for roi_name, mean in roi_means.items():
            if mean is None:
                continue
            key = (roi_name, d.year, d.month)
            ag = accum.setdefault(key, {"sum": 0.0, "n_days": 0})
            ag["sum"] += mean
            ag["n_days"] += 1
        if (i + 1) % 200 == 0:
            print(f"  ... {i + 1}/{len(keys)} files processed")

    rows: list[dict] = []
    for (roi_name, y, m), ag in accum.items():
        n_days = ag["n_days"]
        if n_days < 15:
            no2_mol_m2 = None
        else:
            no2_molec_cm2 = ag["sum"] / n_days
            # molec/cm^2 -> molec/m^2 (×1e4) -> mol/m^2 (÷ Avogadro)
            no2_mol_m2 = no2_molec_cm2 * 1e4 / AVOGADRO
        rows.append({
            "date": pd.Timestamp(year=y, month=m, day=1),
            "roi": roi_name,
            "no2_tropos_col_mol_m2": no2_mol_m2,
            "n_valid_days": n_days,
            "sensor": "OMI",
        })

    df = pd.DataFrame(rows).sort_values(["roi", "date"])
    df.to_csv(out_path, index=False)
    print(f"[ok] OMI: {len(df)} monthly rows -> {out_path}")
    print(df.groupby("roi")["n_valid_days"].agg(["min", "median", "max"]))


if __name__ == "__main__":
    main()
