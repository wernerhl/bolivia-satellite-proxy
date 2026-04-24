"""Stream 2 — calibrate VNF Chaco total RH against YPFB national gas output.

Computes:
  * per-field-sum monthly total RH (MW)
  * rolling 12-month correlation between log(Σ RH) and log(YPFB production)
  * elasticity β from log(prod) = α + β·log(RH) + ε with HAC SEs
  * residual and manipulation-flag series (±2σ, 2 consecutive months)

Writes vnf_chaco_anomaly.csv with (date, rh_total, rh_anomaly_z, residual,
flag_manip). Missing YPFB months are tolerated; we still compute anomalies
against the in-sample residual distribution.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import statsmodels.api as sm

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from _common import abs_path, load_env, paths  # noqa: E402

load_env()


def load_ypfb(path: Path) -> pd.DataFrame:
    """YPFB monthly MMm³/day (expected columns: date, gas_prod_mmm3d)."""
    if not path.exists():
        return pd.DataFrame(columns=["date", "gas_prod_mmm3d"])
    df = pd.read_csv(path, parse_dates=["date"])
    return df[["date", "gas_prod_mmm3d"]]


def main() -> None:
    p = paths()
    monthly = pd.read_csv(abs_path(p["data"]["vnf_monthly"]), parse_dates=["date"])
    if monthly.empty:
        pd.DataFrame(columns=[
            "date", "rh_total", "log_rh", "rh_anomaly_z", "residual", "flag_manip"
        ]).to_csv(abs_path(p["data"]["vnf_anomaly"]), index=False)
        print("[ok] no VNF monthly data; wrote empty anomaly")
        return

    total = monthly.groupby("date", as_index=False)["rh_mw_sum"].sum().rename(
        columns={"rh_mw_sum": "rh_total"}
    )
    total = total.sort_values("date").reset_index(drop=True)
    total = total[total["rh_total"] > 0]
    total["log_rh"] = np.log(total["rh_total"])

    # Z-score on the full sample (no pre-2020 natural baseline for flaring)
    mu, sd = total["log_rh"].mean(), total["log_rh"].std(ddof=1)
    total["rh_anomaly_z"] = (total["log_rh"] - mu) / (sd if sd > 0 else 1.0)

    ypfb = load_ypfb(abs_path(p["data"]["official_ypfb"]))
    joined = total.merge(ypfb, on="date", how="left")
    joined["residual"] = np.nan

    fit = joined.dropna(subset=["gas_prod_mmm3d"])
    if len(fit) >= 24:
        X = sm.add_constant(fit["log_rh"])
        y = np.log(fit["gas_prod_mmm3d"])
        model = sm.OLS(y, X).fit(cov_type="HAC", cov_kwds={"maxlags": 6})
        beta = float(model.params["log_rh"])
        alpha = float(model.params["const"])
        resid = np.log(joined["gas_prod_mmm3d"]) - (alpha + beta * joined["log_rh"])
        joined["residual"] = resid
        print(f"[calib] β={beta:.3f}  α={alpha:.3f}  n={len(fit)}")

        # Rolling 12-month correlation on log levels
        rolling_corr = (joined[["log_rh", "gas_prod_mmm3d"]].assign(
            log_prod=np.log(joined["gas_prod_mmm3d"])
        )["log_rh"].rolling(12)
            .corr(np.log(joined["gas_prod_mmm3d"])))
        joined["rolling12_corr"] = rolling_corr

    # Manipulation flag: residual |z| > 2 for two consecutive months
    r = joined["residual"]
    if r.notna().any():
        rstd = r.std(ddof=1)
        z = r / (rstd if rstd > 0 else 1.0)
        above = z.abs() > 2
        joined["flag_manip"] = (above & above.shift(1)).fillna(False)
    else:
        joined["flag_manip"] = False

    out = joined[[
        "date", "rh_total", "log_rh", "rh_anomaly_z", "residual", "flag_manip",
    ] + (["rolling12_corr"] if "rolling12_corr" in joined.columns else [])]
    out_path = abs_path(p["data"]["vnf_anomaly"])
    out.to_csv(out_path, index=False)
    print(f"[ok] wrote {out_path} ({len(out)} rows)")


if __name__ == "__main__":
    main()
