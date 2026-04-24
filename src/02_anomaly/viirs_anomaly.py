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


def linear_trend_coefs(df_base: pd.DataFrame) -> tuple[float, float]:
    """Fit log(SOL) ~ a + b*t on baseline. `t` is month index from start."""
    t = np.arange(len(df_base))
    y = np.log(df_base["sol"].to_numpy())
    b, a = np.polyfit(t, y, 1)
    return a, b


def per_city(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    df = df.sort_values("date").copy()
    df["date"] = pd.to_datetime(df["date"])
    df = df[df["sol"].notna() & (df["sol"] > 0)]

    base_start = pd.Timestamp(cfg["baseline_start"])
    base_end = pd.Timestamp(cfg["baseline_end"])
    base = df[(df["date"] >= base_start) & (df["date"] <= base_end)]
    if len(base) < 48:
        return df.assign(log_sol=np.nan, trend_extrap=np.nan,
                         anomaly=np.nan, stl_sa=np.nan, stl_seasonal=np.nan)

    a, b = linear_trend_coefs(base)
    # month index anchored to baseline_start
    df["t_idx"] = ((df["date"].dt.year - base_start.year) * 12
                   + (df["date"].dt.month - base_start.month))
    df["log_sol"] = np.log(df["sol"])
    df["trend_extrap"] = a + b * df["t_idx"]
    df["anomaly"] = df["log_sol"] - df["trend_extrap"]

    stl = STL(df.set_index("date")["log_sol"], period=12, robust=True).fit()
    df["stl_seasonal"] = stl.seasonal.values
    df["stl_sa"] = (stl.trend + stl.resid).values
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
