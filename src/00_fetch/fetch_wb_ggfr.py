"""World Bank Global Gas Flaring Reduction (GGFR) annual Bolivia extraction.

Public, free, no registration. Two inputs:
  1. Country-level annual flare volumes (BCM) and flaring intensities
  2. Location-level annual flare volumes (by lat/lon)

We filter each to Bolivia (country) and to the Chaco bounding box (location),
and persist to data/official/ for the VNF calibration cross-check.
"""
from __future__ import annotations

import io
import sys
from pathlib import Path

import pandas as pd
import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from _common import abs_path, ensure_dir, load_env, paths  # noqa: E402

load_env()


def _download(url: str) -> bytes:
    r = requests.get(url, timeout=120)
    r.raise_for_status()
    return r.content


def _read_all_sheets(blob: bytes) -> dict[str, pd.DataFrame]:
    return pd.read_excel(io.BytesIO(blob), sheet_name=None, engine="openpyxl")


def _find_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    lower = {c: c.lower().strip() for c in df.columns.astype(str)}
    for cand in candidates:
        for real, low in lower.items():
            if cand in low:
                return real
    return None


def extract_country(sheets: dict[str, pd.DataFrame], country: str) -> pd.DataFrame:
    """Long-format Bolivia annual series: year, flare_volume_bcm, flaring_intensity."""
    out_rows: list[dict] = []
    for sheet_name, df in sheets.items():
        if df.empty:
            continue
        country_col = _find_col(df, ["country", "economy"])
        if country_col is None:
            continue
        mask = df[country_col].astype(str).str.lower().str.contains(country.lower(), na=False)
        sel = df[mask]
        if sel.empty:
            continue
        year_cols = [c for c in df.columns if str(c).isdigit() and 2012 <= int(str(c)) <= 2030]
        if year_cols:
            for _, row in sel.iterrows():
                for yc in year_cols:
                    val = row[yc]
                    if pd.notna(val):
                        out_rows.append({
                            "year": int(str(yc)),
                            "metric": sheet_name.strip().lower().replace(" ", "_"),
                            "value": float(val),
                        })
        else:
            year_col = _find_col(df, ["year"])
            volume_col = _find_col(df, ["volume", "bcm"])
            intensity_col = _find_col(df, ["intensity"])
            if year_col and (volume_col or intensity_col):
                for _, row in sel.iterrows():
                    rec = {"year": int(row[year_col])}
                    if volume_col and pd.notna(row[volume_col]):
                        rec["flare_volume_bcm"] = float(row[volume_col])
                    if intensity_col and pd.notna(row[intensity_col]):
                        rec["flaring_intensity"] = float(row[intensity_col])
                    out_rows.append(rec)

    if not out_rows:
        return pd.DataFrame(columns=["year", "flare_volume_bcm", "flaring_intensity"])

    df_long = pd.DataFrame(out_rows)
    if "metric" in df_long.columns:
        wide = df_long.pivot_table(index="year", columns="metric",
                                   values="value", aggfunc="first").reset_index()
        wide.columns.name = None
        # Normalize column names
        rename_map = {}
        for c in wide.columns:
            lc = str(c).lower()
            if "volume" in lc or "bcm" in lc:
                rename_map[c] = "flare_volume_bcm"
            elif "intensity" in lc:
                rename_map[c] = "flaring_intensity"
        return wide.rename(columns=rename_map).sort_values("year")
    return df_long.sort_values("year")


def extract_flares_bolivia(sheets: dict[str, pd.DataFrame], bbox: list[float]) -> pd.DataFrame:
    """Filter location-level detections to the Chaco bbox.
    bbox = [minLon, minLat, maxLon, maxLat]."""
    out = []
    for name, df in sheets.items():
        if df.empty:
            continue
        lat_col = _find_col(df, ["lat"])
        lon_col = _find_col(df, ["lon", "long"])
        if lat_col is None or lon_col is None:
            continue
        sel = df[
            df[lat_col].between(bbox[1], bbox[3])
            & df[lon_col].between(bbox[0], bbox[2])
        ].copy()
        if sel.empty:
            continue
        sel["sheet"] = name
        out.append(sel)
    return pd.concat(out, ignore_index=True) if out else pd.DataFrame()


def main() -> None:
    p = paths()
    cfg = p["wb_ggfr"]

    country_csv = abs_path(p["data"]["wb_ggfr_country"])
    flares_csv = abs_path(p["data"]["wb_ggfr_flares"])
    ensure_dir(country_csv.parent)

    print(f"[..] downloading country xlsx ({cfg['country_xlsx_url'][-60:]})")
    country_blob = _download(cfg["country_xlsx_url"])
    country_sheets = _read_all_sheets(country_blob)
    country_df = extract_country(country_sheets, cfg["country_name"])
    country_df.to_csv(country_csv, index=False)
    print(f"[ok] Bolivia annual: {len(country_df)} rows → {country_csv}")

    print(f"[..] downloading flares xlsx")
    flares_blob = _download(cfg["flares_xlsx_url"])
    flares_sheets = _read_all_sheets(flares_blob)
    bbox = p["streams"]["vnf"]["bbox"]
    flares_df = extract_flares_bolivia(flares_sheets, bbox)
    flares_df.to_csv(flares_csv, index=False)
    print(f"[ok] Chaco flares: {len(flares_df)} rows → {flares_csv}")


if __name__ == "__main__":
    main()
