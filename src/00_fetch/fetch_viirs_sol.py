"""Stream 1 — VIIRS DNB monthly sum-of-lights per city buffer.

VNP46A3 (BRDF-corrected monthly composite) is not exposed on Earth
Engine. We roll up VNP46A2 (daily, BRDF+lunar-corrected, gap-filled)
to a monthly mean-radiance image per city, masking low-quality pixels
(Mandatory_Quality_Flag != 0). The EOG stray-light-corrected monthly
composite (VCMSLCFG, band `avg_rad`) is the fallback when no VNP46A2
images are available for a given month.

"Sum of lights" here is the buffer-integrated mean radiance multiplied
by the buffer's pixel count (equivalent to spatial sum of pixel means
over the month). Per-buffer pixel totals (n_valid, n_total, low-coverage
flag) are recorded for the anomaly step.
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from _common import (  # noqa: E402
    abs_path, buffers, ensure_dir, init_ee, load_env, paths, reporting_cutoff_month,
)

load_env()

import ee  # noqa: E402


def city_geom(city: dict) -> "ee.Geometry":
    return ee.Geometry.Point([city["lon"], city["lat"]]).buffer(city["radius_km"] * 1000)


def _month_end(y: int, m: int) -> tuple[int, int]:
    return (y, m + 1) if m < 12 else (y + 1, 1)


def month_iter(start: str, end_inclusive: str):
    sy, sm = map(int, start.split("-"))
    ey, em = map(int, end_inclusive.split("-"))
    y, m = sy, sm
    while (y, m) <= (ey, em):
        yield y, m
        y, m = _month_end(y, m)


def _reduce_monthly_mean(img: "ee.Image", band: str, geom: "ee.Geometry",
                         cfg: dict, source: str) -> dict:
    masked = img.updateMask(img.gte(cfg["mask_low"]).And(img.lte(cfg["mask_high"])))
    pre_count = img.reduceRegion(
        reducer=ee.Reducer.count(),
        geometry=geom, scale=cfg["scale_m"], maxPixels=int(1e10), bestEffort=True,
    ).getInfo().get(band)
    red = masked.reduceRegion(
        reducer=ee.Reducer.mean()
        .combine(ee.Reducer.median(), sharedInputs=True)
        .combine(ee.Reducer.count(), sharedInputs=True)
        .combine(ee.Reducer.sum(), sharedInputs=True),
        geometry=geom, scale=cfg["scale_m"], maxPixels=int(1e10), bestEffort=True,
    ).getInfo()
    n_valid = red.get(f"{band}_count") or 0
    n_total = pre_count or 0
    return {
        "source": source,
        "sol": red.get(f"{band}_sum"),
        "mean_rad": red.get(f"{band}_mean"),
        "median_rad": red.get(f"{band}_median"),
        "n_valid_pixels": n_valid,
        "n_total_pixels": n_total,
        "n_masked": max(0, n_total - n_valid) if n_total else None,
    }


def fetch_primary(year: int, month: int, geom: "ee.Geometry", cfg: dict,
                  min_days: int = 10) -> dict | None:
    """Build a monthly mean from VNP46A2 daily, masking
    Mandatory_Quality_Flag > 1. Returns None when fewer than `min_days`
    daily images are available, forcing the caller to use the fallback
    monthly composite. This handles the 2023-2024 VNP46A2 ingestion gap
    where some months cover only a handful of days over Bolivia."""
    band = cfg["primary_band"]
    qband = cfg["primary_quality_band"]
    start = f"{year}-{month:02d}-01"
    ny, nm = _month_end(year, month)
    end = f"{ny}-{nm:02d}-01"
    coll = ee.ImageCollection(cfg["primary_collection"]).filterDate(start, end).filterBounds(geom)
    n_days = coll.size().getInfo()
    if n_days < min_days:
        return None

    def mask_quality(img: "ee.Image") -> "ee.Image":
        # Mandatory_Quality_Flag: 0 = high quality main, 1 = high quality stray-
        # light corrected, 2 = reduced quality, 255 = poor/no retrieval.
        q = img.select(qband)
        return img.select(band).updateMask(q.lte(1))

    monthly = coll.map(mask_quality).mean()
    result = _reduce_monthly_mean(monthly, band, geom, cfg, cfg["primary_collection"])
    if result and (result.get("n_valid_pixels") or 0) == 0:
        return None
    if result:
        result["n_days_in_month"] = n_days
    return result


def fetch_fallback(year: int, month: int, geom: "ee.Geometry", cfg: dict) -> dict | None:
    band = cfg["fallback_band"]
    start = f"{year}-{month:02d}-01"
    ny, nm = _month_end(year, month)
    end = f"{ny}-{nm:02d}-01"
    coll = ee.ImageCollection(cfg["fallback_collection"]).select(band).filterDate(start, end)
    if coll.size().getInfo() == 0:
        return None
    img = coll.first()
    return _reduce_monthly_mean(img, band, geom, cfg, cfg["fallback_collection"])


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
                    "sol": None, "n_valid_pixels": 0, "n_total_pixels": 0,
                    "mean_rad": None, "median_rad": None, "n_masked": None,
                    "low_coverage_flag": True, "source": "missing",
                })
                continue
            n_valid = r.get("n_valid_pixels") or 0
            n_total = r.get("n_total_pixels") or 0
            low_cov = bool(n_total and (n_valid / n_total) < 0.50)
            rows.append({
                "date": date_str, "city": city["name"],
                "sol": r["sol"], "n_valid_pixels": n_valid,
                "n_total_pixels": n_total, "mean_rad": r["mean_rad"],
                "median_rad": r["median_rad"], "n_masked": r.get("n_masked"),
                "low_coverage_flag": low_cov, "source": r["source"],
            })
            time.sleep(0.05)
        done = sum(1 for r in rows if r["city"] == city["name"])
        print(f"[ok] {city['name']}: {done} months")

    pd.DataFrame(rows).to_csv(out_path, index=False)
    print(f"[ok] wrote {out_path} ({len(rows)} rows)")


if __name__ == "__main__":
    main()
