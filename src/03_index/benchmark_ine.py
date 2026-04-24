"""Benchmark regression: log(IGAE) = α + β₁·viirs_z + β₂·vnf_z + β₃·no2_z + γ·X + ε.

Controls: lagged IGAE, month dummies, and the dollar parallel-market premium
(column `dollar_premium` in official_igae.csv if present). HAC standard errors.
Writes a JSON artifact capturing the coefficients and a two-month-sign-flip
flag used by the monthly report.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import statsmodels.api as sm

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from _common import abs_path, load_env, paths  # noqa: E402

load_env()


def run_benchmark(db_path: Path) -> dict:
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        ci = con.execute("SELECT * FROM ci").df()
    except duckdb.CatalogException:
        return {"status": "no_ci"}
    try:
        igae = con.execute("SELECT * FROM igae").df()
    except duckdb.CatalogException:
        return {"status": "no_igae"}
    con.close()

    ci["date"] = pd.to_datetime(ci["date"])
    igae["date"] = pd.to_datetime(igae["date"])

    df = igae.merge(ci, on="date", how="inner").sort_values("date").reset_index(drop=True)
    if len(df) < 12:
        return {"status": "insufficient_n", "n": int(len(df))}

    df["log_igae"] = np.log(df["igae"])
    df["lag_log_igae"] = df["log_igae"].shift(1)
    df["month"] = df["date"].dt.month
    month_dummies = pd.get_dummies(df["month"], prefix="m", drop_first=True).astype(float)

    cols = ["viirs_z", "vnf_z", "no2_z", "lag_log_igae"]
    if "dollar_premium" in df.columns:
        cols.append("dollar_premium")
    X = df[cols].join(month_dummies)
    X = sm.add_constant(X)
    y = df["log_igae"]
    mask = X.notna().all(axis=1) & y.notna()
    if mask.sum() < 12:
        return {"status": "insufficient_clean_n", "n": int(mask.sum())}
    model = sm.OLS(y[mask], X[mask]).fit(cov_type="HAC", cov_kwds={"maxlags": 6})

    coefs = {k: float(model.params.get(k, np.nan)) for k in ("viirs_z", "vnf_z", "no2_z")}
    pvals = {k: float(model.pvalues.get(k, np.nan)) for k in ("viirs_z", "vnf_z", "no2_z")}
    return {
        "status": "ok",
        "n": int(mask.sum()),
        "r2": float(model.rsquared),
        "betas": coefs,
        "pvalues": pvals,
        "formula": "log(igae) = a + b1*viirs_z + b2*vnf_z + b3*no2_z + lag + monthFE (+dollar_premium)",
    }


def main() -> None:
    p = paths()
    db_path = abs_path(p["data"]["ci_db"])
    result = run_benchmark(db_path)
    out_path = abs_path("data/satellite/benchmark_ine.json")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(result, indent=2))
    print(f"[ok] benchmark: {result.get('status')}  →  {out_path}")


if __name__ == "__main__":
    main()
