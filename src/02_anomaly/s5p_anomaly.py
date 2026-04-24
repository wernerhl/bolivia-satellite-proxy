"""Stream 3 — S5P NO₂ multiplicative anomaly against 2019 month-of-year baseline.

Adds a z-score against the 2019 month distribution and flags the December
2025 fuel-subsidy structural break (no smoothing across it).
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
    cfg = p["streams"]["s5p_no2"]
    in_path = abs_path(p["data"]["s5p_monthly"])
    out_path = abs_path(p["data"]["s5p_anomaly"])
    if not in_path.exists():
        out_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(columns=["date", "roi", "no2_tropos_col_mol_m2", "anomaly_mult",
                              "z_vs_2019", "post_subsidy_break", "volcanic_flag"]
                     ).to_csv(out_path, index=False)
        print(f"[warn] S5P monthly input missing; wrote empty {out_path}")
        return
    monthly = pd.read_csv(in_path, parse_dates=["date"])
    monthly = monthly.dropna(subset=["no2_tropos_col_mol_m2"]).copy()
    monthly["month_of_year"] = monthly["date"].dt.month

    base_start = pd.Timestamp(cfg["baseline_start"])
    base_end = pd.Timestamp(cfg["baseline_end"])
    subsidy_break = pd.Timestamp(cfg["subsidy_break"])

    base = monthly[(monthly["date"] >= base_start) & (monthly["date"] <= base_end)]
    # Month-of-year mean for seasonal adjustment; pooled sd across all baseline
    # months for z-scoring (each month-of-year group has n=1 in a single
    # baseline year so per-month sd would be NaN).
    base_mean = base.groupby(["roi", "month_of_year"])["no2_tropos_col_mol_m2"].mean(
        ).rename("base_mean").reset_index()
    base_sd = base.groupby("roi")["no2_tropos_col_mol_m2"].std(ddof=1
        ).rename("base_sd_pooled").reset_index()

    df = monthly.merge(base_mean, on=["roi", "month_of_year"], how="left")
    df = df.merge(base_sd, on="roi", how="left")
    df["anomaly_mult"] = df["no2_tropos_col_mol_m2"] / df["base_mean"] - 1
    df["z_vs_2019"] = (df["no2_tropos_col_mol_m2"] - df["base_mean"]) / df["base_sd_pooled"]
    df["post_subsidy_break"] = df["date"] >= subsidy_break
    df["volcanic_flag"] = (
        (df["roi"] == "la_paz_el_alto")
        & (df["no2_tropos_col_mol_m2"] > 5 * df["base_mean"])
    )

    out_path = abs_path(p["data"]["s5p_anomaly"])
    df.to_csv(out_path, index=False)
    print(f"[ok] wrote {out_path} ({len(df)} rows)")


if __name__ == "__main__":
    main()
