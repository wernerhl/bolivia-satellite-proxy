"""Stream 1 — STL deseasonalization and pre-2020 linear-trend anomaly.

Reads viirs_sol_monthly.csv; writes viirs_sol_anomaly.csv with the
log-SOL residual against the linear extrapolation of the 2013-01 .. 2019-12
trend, per city. Period-12 robust STL gives an auxiliary seasonal-adjusted
series for diagnostics.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
from statsmodels.tsa.seasonal import STL

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from _common import abs_path, load_env, paths  # noqa: E402

load_env()


def per_city(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    df = df.sort_values("date").copy()
    df["date"] = pd.to_datetime(df["date"])
    df = df[df["sol"].notna() & (df["sol"] > 0)]
    # Drop months flagged as low-coverage (<50% valid pixels) — their
    # radiance integrals are systematically biased downward by the mask.
    if "low_coverage_flag" in df.columns:
        df = df[~df["low_coverage_flag"].astype(bool)]

    # VCMSLCFG (not BRDF-corrected) systematically reads ~25% lower than
    # VNP46A2 over the same city. Estimate a per-city log-level shift from
    # mean difference and add it to fallback rows. This is a crude
    # alignment; a calibration scatter on overlap months would be more
    # rigorous but we don't fetch both products for the same month.
    if "source" in df.columns and df["source"].nunique() > 1:
        primary = cfg["primary_collection"]
        fallback = cfg["fallback_collection"]
        log_sol = np.log(df["sol"])
        mp = log_sol[df["source"] == primary].mean()
        mf = log_sol[df["source"] == fallback].mean()
        if np.isfinite(mp) and np.isfinite(mf):
            shift = mp - mf
            df.loc[df["source"] == fallback, "sol"] = (
                df.loc[df["source"] == fallback, "sol"] * np.exp(shift)
            )

    base_start = pd.Timestamp(cfg["baseline_start"])
    base_end = pd.Timestamp(cfg["baseline_end"])
    if len(df) < 48 or df[df["date"].between(base_start, base_end)].shape[0] < 36:
        return df.assign(log_sol=np.nan, trend_extrap=np.nan, anomaly=np.nan,
                         stl_sa=np.nan, stl_seasonal=np.nan)

    df["log_sol"] = np.log(df["sol"])

    # Robust STL on log(SOL) — removes wet-season seasonal structure.
    stl = STL(df.set_index("date")["log_sol"], period=12, robust=True).fit()
    df["stl_seasonal"] = stl.seasonal.values
    df["stl_sa"] = (stl.trend + stl.resid).values  # seasonally adjusted

    # Linear trend of the SA series over baseline, extrapolated.
    base = df[df["date"].between(base_start, base_end)]
    t_base = np.arange(len(base))
    slope, intercept = np.polyfit(t_base, base["stl_sa"].to_numpy(), 1)
    # Anchor t=0 at base_start; extrapolate linearly.
    df["t_idx"] = ((df["date"].dt.year - base_start.year) * 12
                   + (df["date"].dt.month - base_start.month))
    df["trend_extrap"] = intercept + slope * df["t_idx"]
    df["anomaly"] = df["stl_sa"] - df["trend_extrap"]
    return df


def main() -> None:
    p = paths()
    cfg = p["streams"]["viirs_sol"]
    in_path = abs_path(p["data"]["viirs_sol_monthly"])
    out_path = abs_path(p["data"]["viirs_sol_anomaly"])
    if not in_path.exists():
        out_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(columns=["date", "city", "sol", "n_valid_pixels",
                              "log_sol", "trend_extrap", "anomaly",
                              "stl_seasonal", "stl_sa"]).to_csv(out_path, index=False)
        print(f"[warn] VIIRS monthly input missing; wrote empty {out_path}")
        return
    monthly = pd.read_csv(in_path)

    parts = []
    for city, g in monthly.groupby("city"):
        parts.append(per_city(g, cfg).assign(city=city))
    out = pd.concat(parts, ignore_index=True)
    out = out[[
        "date", "city", "sol", "n_valid_pixels",
        "log_sol", "trend_extrap", "anomaly", "stl_seasonal", "stl_sa",
    ]]
    out.to_csv(out_path, index=False)
    print(f"[ok] wrote {out_path} ({len(out)} rows)")


if __name__ == "__main__":
    main()
