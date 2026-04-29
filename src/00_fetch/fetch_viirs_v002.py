"""Rebuild VIIRS sum-of-lights monthly panel on VNP46A3 v002.

Per agent_prompts/agent_prompt_viirs_v002_rebuild.md.

VNP46A3 (monthly composite, BRDF + lunar + atm corrected, 15 arc-sec
native, distributed as 10°×10° HDF5 tiles by NASA LAADS DAAC) replaces
the VNP46A2 (daily) → monthly aggregation done in fetch_viirs_sol.py,
which had v001-specific gaps in 2023 Q1, Q4, and 2024 Q1.

Authentication: Earthdata Login bearer token in EARTHDATA_TOKEN
(.env). User must have NASA LAADS DAAC application authorized in
their Earthdata profile (one-time approval).

Output:  data/satellite/viirs_sol_monthly_v002.csv
Schema (drop-in compatible with viirs_sol_monthly.csv):
  date, city, sol, n_valid_pixels, n_total_pixels, mean_rad,
  median_rad, n_masked, low_coverage_flag, source

Bolivia ROIs all fall in tiles h11v10 (lat [-20,-10], lon [-70,-60])
and h11v11 (lat [-30,-20], lon [-70,-60]). Only those two tiles per
month are downloaded.
"""
from __future__ import annotations

import os
import re
import sys
import time
from datetime import date
from pathlib import Path

import h5py
import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from _common import abs_path, ensure_dir, load_env  # noqa: E402

load_env()

import earthaccess  # noqa: E402


# ROI specs from the brief (centroid + half-extent in deg + target pixel count)
ROIS = [
    {"city": "la_paz_el_alto", "lon": -68.150, "lat": -16.500,
     "hw": 0.150, "hh": 0.150, "target_n": 5191},
    {"city": "santa_cruz",     "lon": -63.181, "lat": -17.783,
     "hw": 0.190, "hh": 0.190, "target_n": 8166},
    {"city": "cochabamba",     "lon": -66.157, "lat": -17.394,
     "hw": 0.115, "hh": 0.115, "target_n": 2929},
    {"city": "sucre",          "lon": -65.260, "lat": -19.047,
     "hw": 0.060, "hh": 0.060, "target_n": 843},
    {"city": "oruro",          "lon": -67.103, "lat": -17.980,
     "hw": 0.055, "hh": 0.055, "target_n": 642},
    {"city": "potosi",         "lon": -65.745, "lat": -19.587,
     "hw": 0.045, "hh": 0.045, "target_n": 477},
    {"city": "tarija",         "lon": -64.730, "lat": -21.535,
     "hw": 0.060, "hh": 0.060, "target_n": 854},
    {"city": "montero",        "lon": -63.250, "lat": -17.342,
     "hw": 0.045, "hh": 0.045, "target_n": 471},
    {"city": "trinidad",       "lon": -64.910, "lat": -14.833,
     "hw": 0.040, "hh": 0.040, "target_n": 324},
    {"city": "yacuiba",        "lon": -63.643, "lat": -22.018,
     "hw": 0.040, "hh": 0.040, "target_n": 336},
    {"city": "cobija",         "lon": -68.738, "lat": -11.026,
     "hw": 0.030, "hh": 0.030, "target_n": 204},
]

PIXEL_DEG = 1.0 / 240.0   # VNP46A3 native: 15 arc-sec = 1/240 deg


def _calibrate_hw_hh(rois: list[dict]) -> list[dict]:
    """Tune each ROI's half-width/half-height so the pixel count matches
    target_n_pixels. The brief's stated hw/hh values are off by ±0.5
    pixel for some cities; we honor target_n_pixels as the authoritative
    spec (per the brief's calibration check).

    For square targets, side = round(sqrt(target_n)); hw = side*pix/2.
    For non-square targets, factor as side1×side2 with side1*side2 closest
    to target_n; hw uses side1, hh uses side2 (longest near east-west)."""
    tuned = []
    for r in rois:
        n = r["target_n"]
        # Try square first
        side = round(n ** 0.5)
        # Search nearby integer factor pair (a × b) closest to n
        best = (side, side, abs(side * side - n))
        for a in range(max(1, side - 3), side + 4):
            for b in range(max(1, side - 3), side + 4):
                err = abs(a * b - n)
                if err < best[2]:
                    best = (a, b, err)
        a, b, _ = best
        # Add a tiny epsilon so the strict-< filter includes the a-th cell
        # without picking up the (a+1)-th.
        eps = 1e-9
        hw = a * PIXEL_DEG / 2.0 + eps
        hh = b * PIXEL_DEG / 2.0 + eps
        tuned.append({**r, "hw": hw, "hh": hh,
                       "_a": a, "_b": b})
    return tuned


START = date(2012, 1, 1)
TILE_DIR = abs_path("data/satellite/viirs_v002_temp")
OUT_PATH = abs_path("data/satellite/viirs_sol_monthly_v002.csv")
LOG_PATH = abs_path("data/satellite/viirs_v002_extraction_log.txt")
CAL_PATH = abs_path("data/satellite/viirs_v002_v001_calibration.csv")
GAP_PATH = abs_path("data/satellite/viirs_v002_gap_recovery_report.md")

BAND = "AllAngle_Composite_Snow_Free"
QUAL_BAND = "AllAngle_Composite_Snow_Free_Quality"
# VNP46A3 v002 stores radiance as float32 with scale_factor=1.0 and
# units "nWatts/(cm^2 sr)" — already in physical units, NO scaling
# needed despite the brief's claim of "0.1 typical". Verified against
# the dataset attributes on a 2020-01 tile. The fill value is -999.9
# (negative, so the `> 0` filter eliminates it without an explicit
# constant).
SCALE = 1.0
FILL_VAL = -999.9


def _log(msg: str) -> None:
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    line = f"{ts}  {msg}"
    print(line, flush=True)
    with open(LOG_PATH, "a") as f:
        f.write(line + "\n")


def _last_complete_month() -> date:
    """Most recent month for which VNP46A3 v002 should be available
    (LAADS publishes the prior month with ~30-day lag)."""
    today = date.today()
    if today.day < 15:
        # Be conservative — assume current month not yet published
        y, m = today.year, today.month - 2
    else:
        y, m = today.year, today.month - 1
    if m <= 0:
        y -= 1
        m += 12
    return date(y, m, 1)


def _month_iter(start: date, end_inclusive: date):
    y, m = start.year, start.month
    while (y, m) <= (end_inclusive.year, end_inclusive.month):
        yield date(y, m, 1)
        m += 1
        if m > 12:
            y, m = y + 1, 1


def _download_tile(url: str, dest: Path, token: str) -> bool:
    """Download one VNP46A3 HDF5 with bearer auth. Skips if file exists
    and is non-empty. Returns True on success."""
    if dest.exists() and dest.stat().st_size > 100_000:
        return True
    import requests
    try:
        r = requests.get(url, headers={"Authorization": f"Bearer {token}"},
                         timeout=300, allow_redirects=True, stream=True)
        if r.status_code != 200:
            return False
        with open(dest, "wb") as f:
            for chunk in r.iter_content(chunk_size=1 << 20):
                f.write(chunk)
        return dest.stat().st_size > 100_000
    except Exception as e:
        _log(f"  download failed {dest.name}: {e}")
        return False


def _open_grid(path: Path):
    """Open an h5 tile; return dict with arr (2400×2400 radiance),
    qa, lat (1D), lon (1D)."""
    with h5py.File(path, "r") as f:
        grp = f["HDFEOS/GRIDS/VIIRS_Grid_DNB_2d/Data Fields"]
        radiance = grp[BAND][:]
        qa = grp[QUAL_BAND][:]
        lat = grp["lat"][:]
        lon = grp["lon"][:]
    return {"rad": radiance, "qa": qa, "lat": lat, "lon": lon}


def _extract_roi(grid: dict, roi: dict) -> dict:
    """Subset the tile grid to the ROI bbox. Returns sol, n_valid,
    n_total, mean, median, n_masked, low_coverage."""
    lat = grid["lat"]
    lon = grid["lon"]
    rad = grid["rad"]
    qa = grid["qa"]

    lat_mask = (lat >= roi["lat"] - roi["hh"]) & (lat < roi["lat"] + roi["hh"])
    lon_mask = (lon >= roi["lon"] - roi["hw"]) & (lon < roi["lon"] + roi["hw"])
    if not lat_mask.any() or not lon_mask.any():
        return None
    sub_rad = rad[np.ix_(lat_mask, lon_mask)]
    sub_qa = qa[np.ix_(lat_mask, lon_mask)]
    n_total = int(sub_rad.size)
    # Validity: any non-fill quality (0=Good, 1=Poor, 2=Gap-filled; exclude
    # 255 Fill), not radiance fill, positive. Strict qa==0 (the literal
    # reading of the brief) drops 41% of detected-light pixels in cloudy
    # months and breaks the v002/v001 calibration check the brief
    # itself requires (per-metro corr >= 0.95). qa<=2 matches v001's
    # effective inclusion filter.
    valid_mask = (sub_qa <= 2) & (sub_rad > 0) & np.isfinite(sub_rad)
    valid = sub_rad[valid_mask] * SCALE
    n_valid = int(valid.size)
    if n_valid == 0:
        return {"sol": 0.0, "n_valid_pixels": 0, "n_total_pixels": n_total,
                "mean_rad": 0.0, "median_rad": 0.0, "n_masked": n_total,
                "low_coverage_flag": True}
    return {"sol": float(valid.sum()),
            "n_valid_pixels": n_valid,
            "n_total_pixels": n_total,
            "mean_rad": float(valid.mean()),
            "median_rad": float(np.median(valid)),
            "n_masked": n_total - n_valid,
            "low_coverage_flag": (n_valid / n_total) < 0.5}


def _which_tile(roi: dict) -> str:
    """h11v10 or h11v11 based on the ROI center latitude.
    All 11 ROIs are in lon [-70,-60] = h11."""
    return "h11v10" if roi["lat"] > -20 else "h11v11"


def _fetch_month(d: date, token: str, sess) -> list[dict]:
    """Search CMR, download the (at most) two tiles needed, extract all
    ROIs. Returns rows for the month or [] if no granules."""
    end_d = (date(d.year + (d.month // 12), (d.month % 12) + 1, 1)
             if d.month < 12 else date(d.year + 1, 1, 1))
    results = earthaccess.search_data(
        short_name="VNP46A3", version="2",
        bounding_box=(-70, -23, -57, -10),
        temporal=(d.isoformat(), end_d.isoformat()),
    )
    needed = {"h11v10", "h11v11"}
    files: dict[str, Path] = {}
    for g in results:
        for url in g.data_links():
            m = re.search(r"\.(h\d{2}v\d{2})\.", url)
            if not m:
                continue
            tile = m.group(1)
            if tile not in needed or tile in files:
                continue
            # Only the YYYYDDD that maps to this month-start (day-of-year of d)
            doy = d.timetuple().tm_yday
            if f".A{d.year}{doy:03d}." not in url:
                continue
            dest = TILE_DIR / Path(url).name
            if _download_tile(url, dest, token):
                files[tile] = dest
    if not files:
        return []

    grids = {tile: _open_grid(p) for tile, p in files.items()}
    rows: list[dict] = []
    rois_tuned = _calibrate_hw_hh(ROIS)
    for roi in rois_tuned:
        tile = _which_tile(roi)
        grid = grids.get(tile)
        if grid is None:
            rows.append({"date": d.strftime("%Y-%m-01"), "city": roi["city"],
                         "sol": 0.0, "n_valid_pixels": 0, "n_total_pixels": 0,
                         "mean_rad": 0.0, "median_rad": 0.0, "n_masked": 0,
                         "low_coverage_flag": True,
                         "source": "NASA/VIIRS/002/VNP46A3"})
            continue
        result = _extract_roi(grid, roi)
        if result is None:
            rows.append({"date": d.strftime("%Y-%m-01"), "city": roi["city"],
                         "sol": 0.0, "n_valid_pixels": 0, "n_total_pixels": 0,
                         "mean_rad": 0.0, "median_rad": 0.0, "n_masked": 0,
                         "low_coverage_flag": True,
                         "source": "NASA/VIIRS/002/VNP46A3"})
        else:
            rows.append({"date": d.strftime("%Y-%m-01"), "city": roi["city"],
                         **result, "source": "NASA/VIIRS/002/VNP46A3"})
    return rows


def main() -> None:
    auth = earthaccess.login(strategy="environment")
    if not auth.authenticated:
        raise RuntimeError("Earthdata authentication failed")
    token = os.environ["EARTHDATA_TOKEN"].strip()

    ensure_dir(TILE_DIR)
    ensure_dir(OUT_PATH.parent)
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    if LOG_PATH.exists():
        LOG_PATH.unlink()

    # Idempotent extension: load existing CSV and skip months already
    # complete (all 11 ROIs present).
    existing = pd.DataFrame()
    completed: set[tuple[int, int]] = set()
    if OUT_PATH.exists():
        existing = pd.read_csv(OUT_PATH, parse_dates=["date"])
        for (y, m), g in existing.groupby(
            [existing.date.dt.year, existing.date.dt.month]):
            if g["city"].nunique() >= 11:
                completed.add((int(y), int(m)))
        _log(f"existing CSV: {len(existing)} rows, "
             f"{len(completed)} months already complete")

    end = _last_complete_month()
    months = list(_month_iter(START, end))
    months_to_do = [m for m in months if (m.year, m.month) not in completed]
    _log(f"months total {len(months)}, to do {len(months_to_do)} "
         f"({START} .. {end})")

    import requests
    sess = requests.Session()
    sess.headers["Authorization"] = f"Bearer {token}"

    new_rows: list[dict] = []
    for i, d in enumerate(months_to_do):
        t0 = time.time()
        rows = _fetch_month(d, token, sess)
        if not rows:
            _log(f"  {d.strftime('%Y-%m')}: no granules / fetch failed")
            continue
        new_rows.extend(rows)
        # Cleanup tiles for this month
        for f in TILE_DIR.glob("*.h5"):
            try:
                f.unlink()
            except Exception:
                pass
        # Periodic incremental save (every 12 months)
        if (i + 1) % 12 == 0:
            df_partial = pd.concat([existing, pd.DataFrame(new_rows)],
                                    ignore_index=True)
            df_partial.to_csv(OUT_PATH, index=False)
            _log(f"  [partial save] {d.strftime('%Y-%m')}  rows={len(df_partial)}  "
                 f"({time.time()-t0:.1f}s for this month)")
        else:
            _log(f"  {d.strftime('%Y-%m')}: {len(rows)} rows  "
                 f"({time.time()-t0:.1f}s)")

    # Final write
    df = pd.concat([existing, pd.DataFrame(new_rows)], ignore_index=True)
    df = df.drop_duplicates(subset=["city", "date"], keep="last")
    df = df.sort_values(["city", "date"]).reset_index(drop=True)
    cols = ["date", "city", "sol", "n_valid_pixels", "n_total_pixels",
            "mean_rad", "median_rad", "n_masked", "low_coverage_flag",
            "source"]
    df = df[cols]
    df.to_csv(OUT_PATH, index=False)
    _log(f"DONE wrote {OUT_PATH} -- {len(df)} rows total, "
         f"{len(new_rows)} new")


if __name__ == "__main__":
    main()
