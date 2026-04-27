"""OMI/Aura tropospheric NO2 column, daily Level-3 OMNO2d, 2004-10..2021-12.

Two sources, used in this order:

  (1) AWS Open Data registry, public S3 bucket `omi-no2-nasa`
      (us-west-2, no auth). Cloud-Optimized GeoTIFFs re-projected
      from the OMNO2d HDF-EOS5 archive. **Frozen at 2020-05-31** —
      the AWS bucket was a one-time mirror dated 2020-06-01 and
      receives no further updates.

  (2) NASA GES DISC HTTPS endpoint:
      https://acdisc.gesdisc.eosdis.nasa.gov/data/Aura_OMI_Level3/OMNO2d.003/YYYY/
      Native HDF-EOS5 files, requires Earthdata Login. Used for any
      date after the AWS bucket's last file (2020-05-31), so the
      window can extend through 2021-12 and beyond.

Both sources reference the same NASA-produced data; the same band
`ColumnAmountNO2TropCloudScreened` (already cloud-screened upstream
at CloudFraction < 30 percent) feeds the brief's QA spec.

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
GESDISC_BASE = "https://acdisc.gesdisc.eosdis.nasa.gov/data/Aura_OMI_Level3/OMNO2d.003"
AWS_LAST_DATE = date(2020, 5, 31)  # AWS bucket frozen at this date

LON_RES = 0.25
LAT_RES = 0.25
N_LON = 1440
N_LAT = 720
NODATA = -1.2676506e30
AVOGADRO = 6.02214076e23

START = date(2004, 10, 1)   # OMI/Aura L3 OMNO2d archive begins here
END = date(2021, 12, 31)    # Extended for OMI/TROPOMI splice calibration
                            # (TROPOMI starts 2018-07, giving 42 months overlap)
ROI_NAMES = {"la_paz_el_alto", "santa_cruz", "cochabamba"}

FILENAME_RE = re.compile(
    r"OMI-Aura_L3-OMNO2d_(\d{4})m(\d{2})(\d{2})_v003-[\dmt]+\.tif$")


def _list_keys_aws() -> list[tuple[date, str]]:
    """List daily filenames in the AWS S3 bucket (anonymous)."""
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
            if START <= d <= min(END, AWS_LAST_DATE):
                out.append((d, f"{S3_PREFIX}{key}"))
    out.sort(key=lambda t: t[0])
    return out


_GESDISC_LIST_RE = re.compile(
    r'href="(OMI-Aura_L3-OMNO2d_(\d{4})m(\d{2})(\d{2})_v003-[^"]+\.he5)"')


def _list_keys_gesdisc(token: str) -> list[tuple[date, str]]:
    """List daily filenames at GES DISC HTTPS for the date window after
    the AWS bucket's last file. One scraped index per year; the index
    page is stable HTML with `href="OMI-Aura_L3-OMNO2d_*.he5"` entries."""
    import requests
    sess = requests.Session()
    sess.headers["Authorization"] = f"Bearer {token}"
    out: list[tuple[date, str]] = []
    gap_start = max(START, AWS_LAST_DATE + pd.Timedelta(days=1).to_pytimedelta())
    gap_start = date(gap_start.year, gap_start.month, gap_start.day) \
        if not isinstance(gap_start, date) else gap_start
    seen: set[date] = set()
    for year in range(gap_start.year, END.year + 1):
        url = f"{GESDISC_BASE}/{year}/"
        try:
            r = sess.get(url, timeout=60, allow_redirects=True)
        except Exception as e:
            print(f"[warn] GES DISC index {year}: {e}")
            continue
        if r.status_code != 200:
            print(f"[warn] GES DISC index {year}: HTTP {r.status_code}")
            continue
        for fname, y, m, d in _GESDISC_LIST_RE.findall(r.text):
            try:
                day = date(int(y), int(m), int(d))
            except ValueError:
                continue
            if day in seen:
                continue
            if gap_start <= day <= END:
                seen.add(day)
                out.append((day, f"{GESDISC_BASE}/{year}/{fname}"))
    out.sort(key=lambda t: t[0])
    return out


def _list_keys() -> list[tuple[date, str]]:
    """Combined listing: AWS for early dates, GES DISC for the post-2020-05
    window when an EARTHDATA_TOKEN is configured."""
    keys = _list_keys_aws()
    token = os.environ.get("EARTHDATA_TOKEN", "").strip()
    if token and END > AWS_LAST_DATE:
        gesdisc = _list_keys_gesdisc(token)
        print(f"[..] GES DISC: {len(gesdisc)} files in "
              f"({AWS_LAST_DATE+pd.Timedelta(days=1).to_pytimedelta()}, {END}]")
        # Avoid duplicates: only keep GES DISC entries strictly after AWS.
        keys.extend(g for g in gesdisc if g[0] > AWS_LAST_DATE)
    keys.sort(key=lambda t: t[0])
    return keys


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


def _read_all_rois_cog(url: str, windows: dict[str, tuple[Window, int]]
                       ) -> dict[str, float | None]:
    """COG window read (S3 path)."""
    out: dict[str, float | None] = {}
    try:
        with rasterio.open(url) as src:
            for name, (win, _n) in windows.items():
                arr = src.read(1, window=win, masked=False)
                valid = arr[(arr != NODATA) & np.isfinite(arr) & (arr > 0)]
                out[name] = float(valid.mean()) if valid.size else None
    except Exception:
        for name in windows:
            out[name] = None
    return out


def _read_all_rois_he5(url: str, windows: dict[str, tuple[Window, int]],
                        token: str) -> dict[str, float | None]:
    """HDF-EOS5 read for GES DISC files. Streams the file via requests
    (Earthdata bearer auth) into a temp buffer and reads the dataset
    `/HDFEOS/GRIDS/ColumnAmountNO2/Data Fields/ColumnAmountNO2TropCloudScreened`.
    Slower than COG window reads (must download the full ~9 MB file),
    but only ~570 of these total."""
    import io
    import requests
    import h5py
    out: dict[str, float | None] = dict.fromkeys(windows, None)
    try:
        r = requests.get(url, headers={"Authorization": f"Bearer {token}"},
                         timeout=180, allow_redirects=True, stream=False)
    except Exception:
        return out
    if r.status_code != 200:
        return out
    try:
        with h5py.File(io.BytesIO(r.content), "r") as f:
            ds = f["/HDFEOS/GRIDS/ColumnAmountNO2/Data Fields/"
                   "ColumnAmountNO2TropCloudScreened"]
            grid = ds[:]
            # OMNO2d native grid is 720 lat × 1440 lon, lat origin = -90
            # (south-up). The COGs stored at AWS reproject this so row 0
            # is lat = +90 (north-up). Flip rows so window indices match.
            grid = grid[::-1, :]
        for name, (win, _n) in windows.items():
            r0, c0 = win.row_off, win.col_off
            r1, c1 = r0 + win.height, c0 + win.width
            sub = grid[r0:r1, c0:c1]
            valid = sub[(sub != NODATA) & np.isfinite(sub) & (sub > 0)]
            out[name] = float(valid.mean()) if valid.size else None
    except Exception:
        pass
    return out


def _read_all_rois(url: str, windows: dict[str, tuple[Window, int]],
                   token: str = "") -> dict[str, float | None]:
    """Dispatch by URL scheme: COG via /vsis3/ for s3://, HDF5 via
    HTTPS+token for the GES DISC https://."""
    if url.startswith("s3://"):
        return _read_all_rois_cog(url, windows)
    return _read_all_rois_he5(url, windows, token)


def main() -> None:
    out_path = abs_path("data/satellite/no2_omi_monthly.csv")
    ensure_dir(out_path.parent)

    selected = [r for r in rois() if r["name"] in ROI_NAMES]
    windows = {r["name"]: _roi_window(r) for r in selected}

    # Idempotent extension: read any existing rows so we only fetch
    # daily files for (year, month) tuples not already covered.
    existing = pd.DataFrame()
    completed_months: set[tuple[int, int]] = set()
    if out_path.exists():
        try:
            existing = pd.read_csv(out_path, parse_dates=["date"])
            # A month is "completed" if every selected ROI has a row for it.
            for (y, mo), grp in existing.groupby(
                [existing.date.dt.year, existing.date.dt.month]):
                if set(grp["roi"]) >= ROI_NAMES:
                    completed_months.add((int(y), int(mo)))
            print(f"[..] existing CSV: {len(existing)} rows, "
                  f"{len(completed_months)} months already complete")
        except Exception as e:
            print(f"[warn] could not read existing CSV ({e}); refetching all")
            existing = pd.DataFrame()
            completed_months = set()

    print("[..] listing OMI files in S3...")
    all_keys = _list_keys()
    keys = [(d, u) for (d, u) in all_keys
            if (d.year, d.month) not in completed_months]
    skipped = len(all_keys) - len(keys)
    print(f"[ok] {len(all_keys)} daily OMI files in [{START}, {END}]; "
          f"skipping {skipped} already-fetched, processing {len(keys)} new")

    # Accumulate per (roi, year-month)
    accum: dict[tuple, dict] = {}
    last_year = None
    for i, (d, url) in enumerate(keys):
        if d.year != last_year:
            yr_count = sum(1 for k in keys if k[0].year == d.year)
            print(f"[..] {d.year}: {yr_count} files")
            last_year = d.year
        roi_means = _read_all_rois(url, windows, token=os.environ.get(
            "EARTHDATA_TOKEN", ""))
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

    df_new = pd.DataFrame(rows)
    if not existing.empty:
        df = pd.concat([existing, df_new], ignore_index=True)
        # De-dupe in case any (roi, date) overlap (shouldn't happen with
        # the completed_months filter, but guard against re-runs)
        df = df.drop_duplicates(subset=["roi", "date"], keep="last")
    else:
        df = df_new
    df = df.sort_values(["roi", "date"]).reset_index(drop=True)
    df.to_csv(out_path, index=False)
    print(f"[ok] OMI: {len(df)} monthly rows ({len(df_new)} new) -> {out_path}")
    print(df.groupby("roi")["n_valid_days"].agg(["min", "median", "max"]))


if __name__ == "__main__":
    main()
