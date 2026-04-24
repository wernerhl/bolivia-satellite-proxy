"""Stream 4 (Track A) — Sentinel-2 NDVI over five Bolivian cropland zones.

Primary: COPERNICUS/S2_SR_HARMONIZED (2017-03 onward). Cloud mask via
SCL (exclude 3,8,9,10,11). NDVI = (B8-B4)/(B8+B4), monthly median over
valid pixels on the ESA WorldCover 2021 cropland mask (class 40).

Fallback for 2013-01 to 2017-03: LANDSAT/LC08/C02/T1_L2 (B4=red, B5=NIR)
with the Roy et al. (2016) cross-sensor harmonization coefficients
applied to NDVI to splice with S2.

Server-side monthly aggregation (one reduceRegion per zone-month) to
stay under the 5000-element EE collection query ceiling.
"""
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from _common import (  # noqa: E402
    abs_path, ensure_dir, init_ee, load_env, ndvi_zones, paths, reporting_cutoff_month,
)

load_env()

import ee  # noqa: E402


# Roy et al. (2016) NDVI harmonization: NDVI_S2 ≈ 1.00 * NDVI_L8 + 0.01
# Applied to Landsat NDVI values to match Sentinel-2 level.
ROY_INTERCEPT = 0.01
ROY_SLOPE = 1.00


def zone_geom(zone: dict) -> "ee.Geometry":
    return ee.Geometry.Point([zone["lon"], zone["lat"]]).buffer(zone["radius_km"] * 1000)


def crop_mask(cfg: dict) -> "ee.Image":
    wc = ee.ImageCollection(cfg["worldcover"]).first()
    return wc.eq(cfg["worldcover_crop_class"]).selfMask()


def s2_monthly_ndvi(zone: dict, start: date, end: date, cfg: dict) -> pd.DataFrame:
    geom = zone_geom(zone)
    crop = crop_mask(cfg)
    excluded = ee.List(cfg["scl_excluded"])

    def mask_and_ndvi(img: "ee.Image") -> "ee.Image":
        scl = img.select(cfg["scl_band"])
        # keep pixels whose SCL class is NOT in the excluded list
        ok = excluded.map(lambda c: scl.eq(ee.Image.constant(c))).iterate(
            lambda c, acc: ee.Image(acc).Or(ee.Image(c)),
            ee.Image.constant(0),
        )
        clean = img.updateMask(ee.Image(ok).Not()).updateMask(crop)
        ndvi = clean.normalizedDifference(["B8", "B4"]).rename("ndvi")
        return ndvi.copyProperties(img, ["system:time_start"])

    rows: list[dict] = []
    y, m = start.year, start.month
    while (y, m) <= (end.year, end.month):
        nm, ny = (m + 1, y) if m < 12 else (1, y + 1)
        coll = (ee.ImageCollection(cfg["primary_collection"])
                .filterDate(f"{y}-{m:02d}-01", f"{ny}-{nm:02d}-01")
                .filterBounds(geom))
        n = coll.size().getInfo()
        if n == 0:
            rows.append({"date": f"{y:04d}-{m:02d}-01", "zone": zone["name"],
                         "ndvi": None, "n_valid_pixels": 0, "source": "missing"})
            y, m = ny, nm
            continue
        monthly = coll.map(mask_and_ndvi).median()
        red = monthly.reduceRegion(
            reducer=ee.Reducer.mean().combine(ee.Reducer.count(), sharedInputs=True),
            geometry=geom, scale=cfg["scale_m"], maxPixels=int(1e10), bestEffort=True,
        ).getInfo()
        rows.append({
            "date": f"{y:04d}-{m:02d}-01", "zone": zone["name"],
            "ndvi": red.get("ndvi_mean"),
            "n_valid_pixels": red.get("ndvi_count") or 0,
            "source": cfg["primary_collection"],
        })
        y, m = ny, nm
    return pd.DataFrame(rows)


def landsat_monthly_ndvi(zone: dict, start: date, end: date, cfg: dict) -> pd.DataFrame:
    """Landsat-8 C02 L2 monthly composite with QA_PIXEL cloud masking."""
    geom = zone_geom(zone)
    crop = crop_mask(cfg)

    def qa_mask_ndvi(img: "ee.Image") -> "ee.Image":
        qa = img.select(cfg["fallback_qa_band"])
        # bits 3 (cloud), 4 (cloud shadow), 5 (snow)
        cloud = qa.bitwiseAnd(1 << 3).neq(0)
        shadow = qa.bitwiseAnd(1 << 4).neq(0)
        snow = qa.bitwiseAnd(1 << 5).neq(0)
        bad = cloud.Or(shadow).Or(snow)
        clean = img.updateMask(bad.Not()).updateMask(crop)
        red = clean.select(cfg["fallback_red_band"]).multiply(0.0000275).add(-0.2)
        nir = clean.select(cfg["fallback_nir_band"]).multiply(0.0000275).add(-0.2)
        ndvi = nir.subtract(red).divide(nir.add(red)).rename("ndvi_l8")
        return ndvi.copyProperties(img, ["system:time_start"])

    rows: list[dict] = []
    y, m = start.year, start.month
    while (y, m) <= (end.year, end.month):
        nm, ny = (m + 1, y) if m < 12 else (1, y + 1)
        coll = (ee.ImageCollection(cfg["fallback_collection"])
                .filterDate(f"{y}-{m:02d}-01", f"{ny}-{nm:02d}-01")
                .filterBounds(geom))
        n = coll.size().getInfo()
        if n == 0:
            rows.append({"date": f"{y:04d}-{m:02d}-01", "zone": zone["name"],
                         "ndvi": None, "n_valid_pixels": 0, "source": "missing"})
            y, m = ny, nm
            continue
        monthly = coll.map(qa_mask_ndvi).median()
        red = monthly.reduceRegion(
            reducer=ee.Reducer.mean().combine(ee.Reducer.count(), sharedInputs=True),
            geometry=geom, scale=cfg["scale_m"] * 2, maxPixels=int(1e10), bestEffort=True,
        ).getInfo()
        raw_ndvi = red.get("ndvi_l8_mean")
        harmonized = (ROY_SLOPE * raw_ndvi + ROY_INTERCEPT) if raw_ndvi is not None else None
        rows.append({
            "date": f"{y:04d}-{m:02d}-01", "zone": zone["name"],
            "ndvi": harmonized,
            "n_valid_pixels": red.get("ndvi_l8_count") or 0,
            "source": cfg["fallback_collection"],
        })
        y, m = ny, nm
    return pd.DataFrame(rows)


def main() -> None:
    init_ee()
    p = paths()
    cfg = p["streams"]["s2_ndvi"]
    out_path = abs_path(p["data"]["s2_ndvi_monthly"])
    ensure_dir(out_path.parent)

    start_primary = date(2017, 3, 1)
    start_fallback = date.fromisoformat(cfg["start"] + "-01")
    end = date.fromisoformat(reporting_cutoff_month() + "-01")

    all_rows: list[pd.DataFrame] = []
    for zone in ndvi_zones():
        # Landsat for 2013-01 .. 2017-02 (baseline coverage)
        landsat_end = date(2017, 2, 28)
        df_l = landsat_monthly_ndvi(zone, start_fallback, landsat_end, cfg)
        print(f"[ok] {zone['name']} L8: {len(df_l)} months")
        # Sentinel-2 for 2017-03 onward
        df_s = s2_monthly_ndvi(zone, start_primary, end, cfg)
        print(f"[ok] {zone['name']} S2: {len(df_s)} months")
        all_rows.append(pd.concat([df_l, df_s], ignore_index=True))

    df = pd.concat(all_rows, ignore_index=True)
    df["date"] = pd.to_datetime(df["date"])
    df.to_csv(out_path, index=False)
    print(f"[ok] wrote {out_path} ({len(df)} rows)")


if __name__ == "__main__":
    main()
