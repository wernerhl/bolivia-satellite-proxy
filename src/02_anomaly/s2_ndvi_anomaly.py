"""Stream 4 — per-zone month-of-year NDVI anomaly and z-score.

anomaly_z = (NDVI_zt - mean(NDVI_z | month-of-year, 2013-2019))
           / sd(NDVI_z across entire baseline)

Pooled sd (not per-month-of-year) because each month-of-year has only
7 observations in a 7-year baseline and the per-group sd is too noisy.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from _common import abs_path, load_env, paths  # noqa: E402

load_env()


def main() -> None:
    p = paths()
    cfg = p["streams"]["s2_ndvi"]
    in_path = abs_path(p["data"]["s2_ndvi_monthly"])
    out_path = abs_path(p["data"]["s2_ndvi_anomaly"])
    if not in_path.exists():
        out_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(columns=["date", "zone", "ndvi", "anomaly_z",
                              "anomaly_level"]).to_csv(out_path, index=False)
        print(f"[warn] NDVI monthly input missing; wrote empty {out_path}")
        return

    df = pd.read_csv(in_path, parse_dates=["date"])
    df = df.dropna(subset=["ndvi"]).copy()
    df["month_of_year"] = df["date"].dt.month

    base_start = pd.Timestamp(cfg["baseline_start"])
    base_end = pd.Timestamp(cfg["baseline_end"])
    base = df[(df["date"] >= base_start) & (df["date"] <= base_end)]

    # Seasonal mean: per zone × month-of-year.
    seas = base.groupby(["zone", "month_of_year"])["ndvi"].mean().rename(
        "seasonal_mean").reset_index()
    # Pooled sd across the whole baseline per zone.
    sd = base.groupby("zone")["ndvi"].std(ddof=1).rename("sd_pooled").reset_index()

    df = df.merge(seas, on=["zone", "month_of_year"], how="left")
    df = df.merge(sd, on="zone", how="left")
    df["anomaly_level"] = df["ndvi"] - df["seasonal_mean"]
    df["anomaly_z"] = df["anomaly_level"] / df["sd_pooled"]

    df.to_csv(out_path, index=False)
    print(f"[ok] wrote {out_path} ({len(df)} rows)")


if __name__ == "__main__":
    main()
