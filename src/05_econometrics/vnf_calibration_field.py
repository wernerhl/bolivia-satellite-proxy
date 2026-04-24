"""Track C — Bolivia-specific VNF-to-gas-production elasticity per Chaco field.

Two specifications, both on the pre-crisis window 2012-2022 (before YPFB
reporting came under credibility pressure):

  (A) First-differenced field panel:
      Δlog GasProd_{f,t} = α_f + β_VNF · Δlog ΣRH_{f,t} + γ_t + ε_{f,t}

  (B) Level-on-level with field FE + field-specific trends:
      log GasProd_{f,t} = α_f + β · log ΣRH_{f,t} + γ_f · t + ε_{f,t}

Decision tree (prompt):
  - β_pooled ∈ [0.7, 1.3] & pre-crisis R² > 0.6 → "volumetric proxy"
  - β_pooled ∈ [0.3, 0.7] OR R² ∈ [0.3, 0.6]   → "capacity-utilization proxy"
  - R² < 0.3                                    → "hydrocarbon-activity indicator"

Writes vnf_calibration_field.json with per-field + pooled estimates,
R², and the string verdict that fill_paper.py uses to pick §3.1.2
language.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import statsmodels.api as sm

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from _common import abs_path, load_env, paths  # noqa: E402

load_env()


PRE_CRISIS = ("2012-01-01", "2022-12-31")


def _ols_hac(y: np.ndarray, X: np.ndarray, maxlags: int = 6) -> dict:
    if len(y) < 12:
        return {"status": "insufficient_n", "n": int(len(y))}
    X = sm.add_constant(X)
    res = sm.OLS(y, X).fit(cov_type="HAC", cov_kwds={"maxlags": maxlags})
    return {
        "status": "ok",
        "n": int(len(y)),
        "beta": float(res.params[1]),
        "se": float(res.bse[1]),
        "r2": float(res.rsquared),
    }


def _verdict(beta_pooled: float | None, r2_pooled: float | None) -> str:
    if beta_pooled is None or r2_pooled is None:
        return "unknown"
    if 0.7 <= beta_pooled <= 1.3 and r2_pooled > 0.6:
        return "volumetric_proxy"
    if r2_pooled < 0.3:
        return "hydrocarbon_activity_indicator"
    if 0.3 <= beta_pooled <= 0.7 or 0.3 <= r2_pooled <= 0.6:
        return "capacity_utilization_proxy"
    return "capacity_utilization_proxy"


def main() -> None:
    p = paths()
    vnf = pd.read_csv(abs_path(p["data"]["vnf_monthly"]), parse_dates=["date"])
    ypfb_path = abs_path("data/official/ypfb_field_month.csv")
    out = abs_path("data/satellite/vnf_calibration_field.json")
    out.parent.mkdir(parents=True, exist_ok=True)

    if not ypfb_path.exists() or vnf.empty:
        out.write_text(json.dumps({"status": "inputs_missing",
            "needs": ["data/official/ypfb_field_month.csv (date,field,gas_prod_mmm3d)",
                      "populated data/satellite/vnf_chaco_monthly.csv"],
            "verdict": "unknown",
        }, indent=2))
        print(f"[warn] inputs missing → {out}")
        return

    ypfb = pd.read_csv(ypfb_path, parse_dates=["date"])
    df = vnf.merge(ypfb, on=["date", "field"])
    df = df[(df["rh_mw_sum"] > 0) & (df["gas_prod_mmm3d"] > 0)]
    df = df[df["date"].between(*PRE_CRISIS)].copy()
    df["log_rh"] = np.log(df["rh_mw_sum"])
    df["log_gas"] = np.log(df["gas_prod_mmm3d"])

    per_field_A: dict = {}
    per_field_B: dict = {}
    for field, g in df.groupby("field"):
        g = g.sort_values("date")
        # Spec A: first differences
        dy = g["log_gas"].diff().dropna().to_numpy()
        dx = g["log_rh"].diff().dropna().to_numpy()
        per_field_A[field] = _ols_hac(dy, dx.reshape(-1, 1))
        # Spec B: levels + time trend
        t = np.arange(len(g))
        X = np.column_stack([g["log_rh"].to_numpy(), t])
        per_field_B[field] = _ols_hac(g["log_gas"].to_numpy(), X)

    # Pooled spec A with field fixed effects via demeaning within field.
    df_sorted = df.sort_values(["field", "date"]).copy()
    df_sorted["dlog_gas"] = df_sorted.groupby("field")["log_gas"].diff()
    df_sorted["dlog_rh"] = df_sorted.groupby("field")["log_rh"].diff()
    pooled_panel = df_sorted.dropna(subset=["dlog_gas", "dlog_rh"])
    pooled_A = _ols_hac(pooled_panel["dlog_gas"].to_numpy(),
                        pooled_panel["dlog_rh"].to_numpy().reshape(-1, 1))

    # Pooled spec B: levels with field FE (dummies, drop first)
    fe = pd.get_dummies(df_sorted["field"], drop_first=True).astype(float)
    X_b = np.column_stack([df_sorted["log_rh"].to_numpy(), fe.to_numpy()])
    pooled_B = _ols_hac(df_sorted["log_gas"].to_numpy(), X_b)

    result = {
        "sample": f"{PRE_CRISIS[0]} .. {PRE_CRISIS[1]}",
        "n_field_months": int(len(df)),
        "per_field_first_diff": per_field_A,
        "per_field_level_with_trend": per_field_B,
        "pooled_first_diff": pooled_A,
        "pooled_level_with_fe": pooled_B,
        "verdict": _verdict(
            pooled_A.get("beta") if pooled_A.get("status") == "ok" else None,
            pooled_A.get("r2") if pooled_A.get("status") == "ok" else None,
        ),
    }
    out.write_text(json.dumps(result, indent=2))
    print(f"[ok] VNF calibration → verdict={result['verdict']}  {out}")


if __name__ == "__main__":
    main()
