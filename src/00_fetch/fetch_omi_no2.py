"""OMI/Aura tropospheric NO2 column, daily Level-3 OMNO2d, 2005-01..2018-06.

NOTE — DEVIATION FROM BRIEF
The agent brief said "Source: GEE, OMI Level-3 daily NO₂". OMI/OMNO2d is
NOT in the public Google Earth Engine catalog (neither the official
NASA collections nor the awesome-gee-community-catalog hosts it as of
April 2026). The closest authoritative source is NASA GES DISC, which
serves the OMNO2d HDF-EOS5 daily files via HTTPS and OPeNDAP and
requires an Earthdata Login.

This fetcher targets the GES DISC HTTPS endpoint:
  https://acdisc.gesdisc.eosdis.nasa.gov/data/Aura_OMI_Level3/OMNO2d.003/

Authentication via Earthdata Login (urs.earthdata.nasa.gov):
  EARTHDATA_USER + EARTHDATA_PASS in .env, OR
  EARTHDATA_TOKEN (bearer)
The user must authorize the "NASA GESDISC DATA ARCHIVE" application
in their Earthdata profile before any download succeeds.

Output schema (per brief):
  date, roi, no2_tropos_col_mol_m2, n_valid_days, sensor

QA filter: keep daily-grid pixels with CloudFraction < 0.30 (the OMNO2d
"ColumnAmountNO2TropCloudScreened" band is the cloud-screened L3
already at the brief's 0.30 threshold; we still record n_valid_days
based on the count of daily files with at least one valid pixel inside
the ROI).
"""
from __future__ import annotations

import io
import os
import re
import sys
from datetime import date, timedelta
from pathlib import Path

import netrc
import numpy as np
import pandas as pd
import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from _common import abs_path, ensure_dir, load_env, rois  # noqa: E402

load_env()


GESDISC_BASE = ("https://acdisc.gesdisc.eosdis.nasa.gov/data/"
                "Aura_OMI_Level3/OMNO2d.003")
EARTHDATA_LOGIN = "https://urs.earthdata.nasa.gov"

# Fixed grid: 0.25° × 0.25°, longitude -180..180, latitude -90..90
LON_RES = 0.25
LAT_RES = 0.25
N_LON = 1440
N_LAT = 720

START = date(2004, 10, 1)   # OMI/Aura first full month (Aura launched 2004-07)
END = date(2018, 6, 30)

ROI_NAMES = {"la_paz_el_alto", "santa_cruz", "cochabamba"}


def _earthdata_session() -> requests.Session:
    """Build a requests session that follows the Earthdata Login redirect."""
    s = requests.Session()
    token = os.environ.get("EARTHDATA_TOKEN")
    if token:
        s.headers.update({"Authorization": f"Bearer {token}"})
        return s
    user = os.environ.get("EARTHDATA_USER")
    pw = os.environ.get("EARTHDATA_PASS")
    if not (user and pw):
        raise RuntimeError(
            "Earthdata credentials missing. Set EARTHDATA_TOKEN or "
            "EARTHDATA_USER + EARTHDATA_PASS in .env. Register at "
            "https://urs.earthdata.nasa.gov/ and authorize the "
            "'NASA GESDISC DATA ARCHIVE' application.")
    s.auth = (user, pw)
    return s


def _list_files_for_year(year: int, sess: requests.Session) -> list[str]:
    """Scrape the year directory index for OMNO2d HDF5 filenames."""
    url = f"{GESDISC_BASE}/{year}/contents.html"
    r = sess.get(url, timeout=60, allow_redirects=True)
    if r.status_code != 200:
        return []
    return re.findall(r"OMI-Aura_L3-OMNO2d_[\dm]+_v003-[\dmt]+\.he5", r.text)


def _read_hdf5(blob: bytes) -> dict[str, np.ndarray]:
    """Read the cloud-screened tropospheric NO2 band from a single OMNO2d
    HDF-EOS5 file. Returns {'no2': 2D ndarray (1440, 720), 'fill': value}."""
    import h5py
    with h5py.File(io.BytesIO(blob), "r") as f:
        grp = f["/HDFEOS/GRIDS/ColumnAmountNO2/Data Fields"]
        # Brief specifies "tropospheric column" with cloud screening at
        # CloudFraction < 0.30 — exactly what the L3 cloud-screened band
        # implements upstream.
        ds = grp["ColumnAmountNO2TropCloudScreened"]
        arr = ds[:]
        fill = ds.attrs.get("_FillValue",
                              ds.attrs.get("MissingValue", -1.2676506e30))
    return {"no2": arr, "fill": float(fill)}


def _roi_grid_indices(roi: dict) -> tuple[slice, slice, int]:
    """Indices into the 1440 × 720 OMNO2d grid covering this ROI.
    Returns (lon_slice, lat_slice, n_native_pixels)."""
    lon_min = min(roi["nw_lon"], roi["se_lon"])
    lon_max = max(roi["nw_lon"], roi["se_lon"])
    lat_min = min(roi["nw_lat"], roi["se_lat"])
    lat_max = max(roi["nw_lat"], roi["se_lat"])
    # Grid index 0 corresponds to lon = -180, lat = -90.
    i0 = max(0, int((lon_min + 180) / LON_RES))
    i1 = min(N_LON, int(np.ceil((lon_max + 180) / LON_RES)))
    j0 = max(0, int((lat_min + 90) / LAT_RES))
    j1 = min(N_LAT, int(np.ceil((lat_max + 90) / LAT_RES)))
    if i1 == i0:
        i1 = i0 + 1
    if j1 == j0:
        j1 = j0 + 1
    n_pix = (i1 - i0) * (j1 - j0)
    return slice(j0, j1), slice(i0, i1), n_pix


def fetch_one_day(year: int, month: int, day: int,
                  sess: requests.Session) -> bytes | None:
    """Fetch one OMNO2d HDF5 by hitting the year directory and finding the
    file whose date stem matches YYYYmMMDD."""
    stem = f"OMI-Aura_L3-OMNO2d_{year:04d}m{month:02d}{day:02d}"
    # Try a small number of vintage suffixes
    candidates = _list_files_for_year(year, sess)
    match = next((f for f in candidates if f.startswith(stem)), None)
    if not match:
        return None
    url = f"{GESDISC_BASE}/{year}/{match}"
    r = sess.get(url, timeout=120, allow_redirects=True)
    if r.status_code != 200:
        return None
    return r.content


def main() -> None:
    sess = _earthdata_session()
    out_path = abs_path("data/satellite/no2_omi_monthly.csv")
    ensure_dir(out_path.parent)

    selected_rois = [r for r in rois() if r["name"] in ROI_NAMES]
    grid_idx = {r["name"]: _roi_grid_indices(r) for r in selected_rois}

    # Per (roi, year-month): accumulate sums and valid-day counts
    accum: dict[tuple, dict] = {}

    cur = START
    while cur <= END:
        ny, nm = (cur.year, cur.month + 1) if cur.month < 12 else (cur.year + 1, 1)
        # iterate days in month
        d = cur
        while d.month == cur.month and d <= END:
            blob = fetch_one_day(d.year, d.month, d.day, sess)
            if blob is not None:
                try:
                    info = _read_hdf5(blob)
                except Exception as e:
                    print(f"[warn] {d}: HDF parse failed ({e})")
                    info = None
                if info is not None:
                    arr = info["no2"]
                    fill = info["fill"]
                    for r in selected_rois:
                        lat_s, lon_s, _ = grid_idx[r["name"]]
                        sub = arr[lat_s, lon_s]
                        valid = sub[(sub != fill) & np.isfinite(sub)]
                        if valid.size == 0:
                            continue
                        key = (r["name"], cur.year, cur.month)
                        ag = accum.setdefault(key, {"sum": 0.0, "n_pix": 0,
                                                     "n_days": 0})
                        ag["sum"] += valid.mean()
                        ag["n_pix"] = max(ag["n_pix"], int(valid.size))
                        ag["n_days"] += 1
            d = d + timedelta(days=1)
        cur = date(ny, nm, 1)
        # Flush a year of progress per console line
        if cur.month == 1:
            print(f"[..] completed through {(cur - timedelta(days=1)).isoformat()}")

    rows: list[dict] = []
    for (roi_name, y, m), ag in accum.items():
        # n_valid_days >= 15 threshold per brief
        if ag["n_days"] < 15:
            no2 = None
        else:
            no2 = ag["sum"] / ag["n_days"]
        # OMNO2d column units are molec/cm² ; convert to mol/m² for
        # comparability with the TROPOMI series:
        # 1 molec/cm² = 1e4 molec/m² ; molec → mol via Avogadro
        if no2 is not None:
            no2_mol_m2 = no2 * 1e4 / 6.02214076e23
        else:
            no2_mol_m2 = None
        rows.append({
            "date": pd.Timestamp(year=y, month=m, day=1),
            "roi": roi_name,
            "no2_tropos_col_mol_m2": no2_mol_m2,
            "n_valid_days": ag["n_days"],
            "sensor": "OMI",
        })

    df = pd.DataFrame(rows).sort_values(["roi", "date"])
    df.to_csv(out_path, index=False)
    print(f"[ok] OMI: {len(df)} rows -> {out_path}")


if __name__ == "__main__":
    main()
