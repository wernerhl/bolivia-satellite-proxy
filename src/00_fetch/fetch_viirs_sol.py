"""Stream 1 — VIIRS DNB monthly sum-of-lights.

Computes per-city-buffer SOL on Earth Engine using the BRDF-corrected
VNP46A3 monthly product. Falls back to the legacy EOG DNB/MONTHLY_V1
composite when VNP46A3 has a gap > 45 days for a given month.

Writes one row per (date, city) to the monthly CSV.
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from _common import (  # noqa: E402
    abs_path,
    buffers,
    ensure_dir,
    init_ee,
    load_env,
    paths,
    reporting_cutoff_month,
)

load_env()

import ee  # noqa: E402


def city_geom(city: dict) -> "ee.Geometry":
    return ee.Geometry.Point([city["lon"], city["lat"]]).buffer(
        city["radius_km"] * 1000
    )


def _month_end(y: int, m: int) -> tuple[int, int]:
    return (y, m + 1) if m < 12 else (y + 1, 1)


def month_iter(start: str, end_inclusive: str):
    sy, sm = map(int, start.split("-"))
    ey, em = map(int, end_inclusive.split("-"))
    y, m = sy, sm
    while (y, m) <= (ey, em):
        yield y, m
        y, m = _month_end(y, m)


def fetch_primary(year: int, month: int, geom: "ee.Geometry", cfg: dict) -> dict | None:
    coll = (
        ee.ImageCollection(cfg["primary_collection"])
        .select(cfg["primary_band"])
        .filterDate(f"{year}-{month:02d}-01", f"{_month_end(year, month)[0]}-"
                                              f"{_month_end(year, month)[1]:02d}-01")
    )
    n = coll.size().getInfo()
    if n == 0:
        return None
    img = coll.first()
    masked = img.updateMask(img.gte(cfg["mask_low"]).And(img.lte(cfg["mask_high"])))
    red = masked.reduceRegion(
        reducer=ee.Reducer.sum()
        .combine(ee.Reducer.mean(), sharedInputs=True)
        .combine(ee.Reducer.median(), sharedInputs=True)
        .combine(ee.Reducer.count(), sharedInputs=True),
        geometry=geom,
        scale=cfg["scale_m"],
        maxPixels=int(1e10),
        bestEffort=True,
    ).getInfo()
    band = cfg["primary_band"]
    return {
        "source": cfg["primary_collection"],
        "sol": red.get(f"{band}_sum"),
        "mean_rad": red.get(f"{band}_mean"),
        "median_rad": red.get(f"{band}_median"),
        "n_valid_pixels": red.get(f"{band}_count"),
    }


def fetch_fallback(year: int, month: int, geom: "ee.Geometry", cfg: dict) -> dict | None:
    coll = (
        ee.ImageCollection(cfg["fallback_collection"])
        .select(cfg["fallback_band"])
        .filterDate(f"{year}-{month:02d}-01", f"{_month_end(year, month)[0]}-"
                                              f"{_month_end(year, month)[1]:02d}-01")
    )
    if coll.size().getInfo() == 0:
        return None
    img = coll.first()
    masked = img.updateMask(img.gte(cfg["mask_low"]).And(img.lte(cfg["mask_high"])))
    red = masked.reduceRegion(
        reducer=ee.Reducer.sum()
        .combine(ee.Reducer.mean(), sharedInputs=True)
        .combine(ee.Reducer.median(), sharedInputs=True)
        .combine(ee.Reducer.count(), sharedInputs=True),
        geometry=geom,
        scale=cfg["scale_m"],
        maxPixels=int(1e10),
        bestEffort=True,
    ).getInfo()
    band = cfg["fallback_band"]
    return {
        "source": cfg["fallback_collection"],
        "sol": red.get(f"{band}_sum"),
        "mean_rad": red.get(f"{band}_mean"),
        "median_rad": red.get(f"{band}_median"),
        "n_valid_pixels": red.get(f"{band}_count"),
    }


def main() -> None:
    init_ee()
    p = paths()
    cfg = p["streams"]["viirs_sol"]
    cities = buffers()

    out_path = abs_path(p["data"]["viirs_sol_monthly"])
    ensure_dir(out_path.parent)

    start = cfg["start"]
    end = reporting_cutoff_month()

    rows: list[dict] = []
    for city in cities:
        geom = city_geom(city)
        for y, m in month_iter(start, end):
            date_str = f"{y:04d}-{m:02d}-01"
            r = fetch_primary(y, m, geom, cfg) or fetch_fallback(y, m, geom, cfg)
            if r is None:
                rows.append({
                    "date": date_str, "city": city["name"],
                    "sol": None, "n_valid_pixels": 0,
                    "mean_rad": None, "median_rad": None,
                    "n_masked": None, "source": "missing",
                })
                continue
            rows.append({
                "date": date_str,
                "city": city["name"],
                "sol": r["sol"],
                "n_valid_pixels": r["n_valid_pixels"],
                "mean_rad": r["mean_rad"],
                "median_rad": r["median_rad"],
                "n_masked": None,
                "source": r["source"],
            })
            time.sleep(0.05)
        print(f"[ok] {city['name']}: {sum(1 for r in rows if r['city'] == city['name'])} months")

    pd.DataFrame(rows).to_csv(out_path, index=False)
    print(f"[ok] wrote {out_path} ({len(rows)} rows)")


if __name__ == "__main__":
    main()
