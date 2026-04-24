"""Stream 2 — attribute VNF detections to Chaco gas-flare inventory.

Reads all raw JSONL days, filters Cloud_Mask == 0 and Temp_BB >= 1400 K,
assigns each detection to the nearest configured flare within 2 km
(unassigned -> `other_chaco`; dropped in the SW discard zone). Aggregates
daily RH, then monthly per field.
"""
from __future__ import annotations

import json
import math
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from _common import abs_path, ensure_dir, flares, load_env, paths  # noqa: E402

load_env()


EARTH_R_KM = 6371.0088


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return 2 * EARTH_R_KM * math.asin(math.sqrt(a))


def load_jsonl(path: Path) -> list[dict]:
    if path.stat().st_size == 0:
        return []
    rows = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def pick(rec: dict, keys: list[str]):
    for k in keys:
        if k in rec:
            return rec[k]
    return None


def nearest_field(lat: float, lon: float, fields: list[dict], radius_km: float):
    best_d = radius_km + 1
    best = None
    for fld in fields:
        d = haversine_km(lat, lon, fld["lat"], fld["lon"])
        if d < best_d:
            best_d = d
            best = fld["name"]
    return best if best_d <= radius_km else None


def main() -> None:
    p = paths()
    cfg = p["streams"]["vnf"]
    fl = flares()
    fields = fl["fields"]

    raw_dir = abs_path(p["data"]["raw_vnf"])
    out_path = abs_path(p["data"]["vnf_monthly"])
    ensure_dir(out_path.parent)

    rows: list[dict] = []
    if not raw_dir.exists():
        pd.DataFrame(columns=["date", "field", "rh_mw_sum", "n_detections",
                              "mean_temp_bb", "missing_days"]).to_csv(out_path, index=False)
        print(f"[warn] VNF raw dir missing; wrote empty {out_path}")
        return
    for path in sorted(raw_dir.glob("*.jsonl")):
        day = path.stem  # YYYY-MM-DD
        for rec in load_jsonl(path):
            cm = pick(rec, ["Cloud_Mask", "cloud_mask"])
            temp = pick(rec, ["Temp_BB", "temp_bb"])
            rh = pick(rec, ["RH", "rh"])
            lat = pick(rec, ["Lat_GMTCO", "lat_gmtco", "Lat", "lat"])
            lon = pick(rec, ["Lon_GMTCO", "lon_gmtco", "Lon", "lon"])
            if None in (cm, temp, rh, lat, lon):
                continue
            if cm not in cfg["cloud_mask_accept"]:
                continue
            if float(temp) < cfg["temp_bb_min"]:
                continue
            if lat < cfg["discard_sw_lat"] and lon < cfg["discard_sw_lon"]:
                continue
            name = nearest_field(float(lat), float(lon), fields, cfg["attribution_radius_km"])
            rows.append({
                "date": day, "field": name or "other_chaco",
                "rh": float(rh), "temp_bb": float(temp), "lat": lat, "lon": lon,
            })

    if not rows:
        pd.DataFrame(columns=[
            "date", "field", "rh_mw_sum", "n_detections", "mean_temp_bb", "missing_days"
        ]).to_csv(out_path, index=False)
        print(f"[ok] no VNF detections; wrote empty {out_path}")
        return

    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])
    df["month"] = df["date"].dt.to_period("M").dt.to_timestamp()

    # Daily per-field
    daily = df.groupby(["month", "field", df["date"].dt.date]).agg(
        rh_day=("rh", "sum"),
        n_day=("rh", "size"),
        temp_day=("temp_bb", "mean"),
    ).reset_index()

    monthly = daily.groupby(["month", "field"]).agg(
        rh_mw_sum=("rh_day", "sum"),
        n_detections=("n_day", "sum"),
        mean_temp_bb=("temp_day", "mean"),
    ).reset_index().rename(columns={"month": "date"})

    # Missing-days: per field, count absent days in the month vs its expected span.
    span = df.groupby("month")["date"].nunique().rename("days_present")
    expected = df["month"].drop_duplicates().to_frame()
    expected["days_in_month"] = expected["month"].dt.days_in_month
    span_df = expected.merge(span, on="month", how="left").fillna({"days_present": 0})
    span_df["missing_days"] = span_df["days_in_month"] - span_df["days_present"]
    monthly = monthly.merge(
        span_df[["month", "missing_days"]].rename(columns={"month": "date"}),
        on="date", how="left",
    )

    monthly.to_csv(out_path, index=False)
    print(f"[ok] wrote {out_path} ({len(monthly)} rows)")


if __name__ == "__main__":
    main()
