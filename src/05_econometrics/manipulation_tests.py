"""Manipulation-detection suite — paper §4.3.

Three tests, each designed for the single-country Bolivia setting
(the Martinez 2022 cross-country test does not apply directly).

TEST 1 — Pre/post INE-trust break.
  Fit eq (6) VIIRS elasticity separately on "high-trust" (2006–2014)
  and "low-trust" (2020–2024) samples; a significant increase in β on
  the low-trust sample is the Martinez signature.

TEST 2 — Satellite-vs-official residual.
  Regress the satellite DFM factor on INE quarterly GDP growth; test
  whether residuals are systematically signed by period. Chow-style
  split on the reserve-collapse break (2023Q4).

TEST 3 — Sectoral consistency (VNF × YPFB × INE hydrocarbon VA).
  Compare (a) VNF-implied production vs YPFB reported and (b) YPFB vs
  INE hydrocarbon value added. If a agrees and b disagrees → aggregation
  manipulation; if a disagrees → source-data manipulation.

Each test writes its own JSON; a summary JSON combines all three plus
an overall verdict flag.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from _common import abs_path, load_env, paths  # noqa: E402

load_env()


def _ols(X: np.ndarray, y: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    beta = np.linalg.solve(X.T @ X, X.T @ y)
    resid = y - X @ beta
    n, k = X.shape
    sigma2 = (resid @ resid) / (n - k)
    V = sigma2 * np.linalg.inv(X.T @ X)
    return beta, np.sqrt(np.diag(V))


def test1_pre_post_trust_break() -> dict:
    """VIIRS elasticity pre/post trust break. Annual department panel."""
    gdp = abs_path("data/official/ine_gdp_dept.csv")
    sol = abs_path("data/satellite/viirs_sol_dept_annual.csv")
    if not (gdp.exists() and sol.exists()):
        return {"status": "inputs_missing"}

    a = pd.read_csv(gdp); b = pd.read_csv(sol)
    df = a.merge(b, on=["year", "department"])
    df = df[(df["gdp_usd"] > 0) & (df["sol"] > 0)].copy()
    df["log_gdp"] = np.log(df["gdp_usd"])
    df["log_sol"] = np.log(df["sol"])
    df = df.sort_values(["department", "year"])
    df["dlog_gdp"] = df.groupby("department")["log_gdp"].diff()
    df["dlog_sol"] = df.groupby("department")["log_sol"].diff()
    df = df.dropna(subset=["dlog_gdp", "dlog_sol"])

    out: dict = {}
    for name, mask in [
        ("high_trust_2006_2014", df["year"].between(2006, 2014)),
        ("low_trust_2020_2024", df["year"].between(2020, 2024)),
    ]:
        sub = df[mask]
        if len(sub) < 20:
            out[name] = {"status": "insufficient_n", "n": int(len(sub))}
            continue
        X = np.column_stack([np.ones(len(sub)), sub["dlog_sol"].to_numpy()])
        y = sub["dlog_gdp"].to_numpy()
        beta, se = _ols(X, y)
        out[name] = {"n": int(len(sub)), "beta": float(beta[1]), "se": float(se[1])}

    ht = out.get("high_trust_2006_2014", {})
    lt = out.get("low_trust_2020_2024", {})
    verdict = "inconclusive"
    if "beta" in ht and "beta" in lt:
        diff = lt["beta"] - ht["beta"]
        joint_se = float(np.sqrt(ht["se"] ** 2 + lt["se"] ** 2))
        z = diff / joint_se if joint_se > 0 else 0
        out["beta_diff"] = diff
        out["z_diff"] = z
        out["significant_increase"] = bool(z > 1.96)
        verdict = "martinez_signal" if z > 1.96 else "no_signal"
    out["verdict"] = verdict
    out["status"] = "ok"
    return out


def test2_satellite_vs_official_residual() -> dict:
    """Regress factor_z on INE GDP growth; Chow split at 2023Q4."""
    p = paths()
    # Load satellite factor (DFM preferred, fall back to CI)
    dfm = abs_path("data/satellite/dfm_result.json")
    if dfm.exists() and json.loads(dfm.read_text()).get("status") == "ok":
        d = json.loads(dfm.read_text())
        sat = pd.Series(d["factor_z"], index=pd.to_datetime(d["factor_index"]), name="factor")
    else:
        ci = pd.read_csv(abs_path(p["data"]["ci"]), parse_dates=["date"]).dropna(subset=["ci"])
        sat = ci.set_index("date")["ci"].rename("factor")

    gdp_path = abs_path("data/official/ine_gdp_quarterly.csv")
    if not gdp_path.exists():
        return {"status": "inputs_missing"}
    gdp = pd.read_csv(gdp_path, parse_dates=["date"]).set_index("date")
    gdp["growth"] = np.log(gdp["gdp_real"]).diff(4)  # 4-quarter change
    # Align satellite to quarter end
    sat_q = sat.resample("QE").mean()
    df = pd.concat([sat_q, gdp["growth"]], axis=1).dropna()
    if len(df) < 12:
        return {"status": "insufficient_n", "n": int(len(df))}

    X = np.column_stack([np.ones(len(df)), df["growth"].to_numpy()])
    y = df["factor"].to_numpy()
    beta, se = _ols(X, y)
    resid = y - X @ beta
    df["residual"] = resid

    split = pd.Timestamp("2023-10-01")
    pre = df[df.index < split]["residual"]
    post = df[df.index >= split]["residual"]

    return {
        "status": "ok",
        "n": int(len(df)),
        "intercept": float(beta[0]),
        "beta_growth": float(beta[1]),
        "se_beta": float(se[1]),
        "pre_mean": float(pre.mean()) if len(pre) else None,
        "post_mean": float(post.mean()) if len(post) else None,
        "post_mean_t": float(post.mean() / (post.std(ddof=1) / np.sqrt(len(post))))
                       if len(post) > 1 and post.std(ddof=1) > 0 else None,
        "post_sign": "negative (satellite lower than implied by INE)"
                    if len(post) and post.mean() < 0
                    else "positive (satellite higher than implied by INE)",
    }


def test3_sectoral_consistency() -> dict:
    """VNF → implied production vs YPFB vs INE hydrocarbon VA."""
    p = paths()
    vnf = pd.read_csv(abs_path(p["data"]["vnf_monthly"]), parse_dates=["date"])
    ypfb_path = abs_path(p["data"]["official_ypfb"])
    ine_hydro = abs_path("data/official/ine_hydrocarbon_va.csv")

    if not ypfb_path.exists() or not ine_hydro.exists() or vnf.empty:
        return {"status": "inputs_missing"}

    ypfb = pd.read_csv(ypfb_path, parse_dates=["date"])
    ine = pd.read_csv(ine_hydro, parse_dates=["date"])

    vnf_total = vnf.groupby("date", as_index=False)["rh_mw_sum"].sum()
    df = vnf_total.merge(ypfb, on="date").merge(ine, on="date")
    df = df[(df["rh_mw_sum"] > 0) & (df["gas_prod_mmm3d"] > 0) & (df["hydrocarbon_va"] > 0)]
    if len(df) < 24:
        return {"status": "insufficient_n", "n": int(len(df))}

    # Agreement (a): VNF–YPFB log-log correlation
    r_vnf_ypfb = float(np.corrcoef(np.log(df["rh_mw_sum"]),
                                   np.log(df["gas_prod_mmm3d"]))[0, 1])
    # Agreement (b): YPFB–INE hydrocarbon VA correlation
    r_ypfb_ine = float(np.corrcoef(np.log(df["gas_prod_mmm3d"]),
                                    np.log(df["hydrocarbon_va"]))[0, 1])

    a_agrees = r_vnf_ypfb >= 0.80
    b_agrees = r_ypfb_ine >= 0.80

    if a_agrees and not b_agrees:
        verdict = "aggregation_manipulation"
    elif not a_agrees:
        verdict = "source_data_manipulation"
    elif a_agrees and b_agrees:
        verdict = "no_manipulation_detected"
    else:
        verdict = "inconclusive"

    return {
        "status": "ok", "n": int(len(df)),
        "vnf_ypfb_corr": r_vnf_ypfb, "ypfb_ine_corr": r_ypfb_ine,
        "verdict": verdict,
    }


def main() -> None:
    out = {
        "test1_pre_post": test1_pre_post_trust_break(),
        "test2_residual": test2_satellite_vs_official_residual(),
        "test3_sectoral": test3_sectoral_consistency(),
    }
    signals = [r.get("verdict") for r in out.values() if isinstance(r, dict)]
    out["overall"] = {
        "any_manipulation_signal": any(s in ("martinez_signal",
                                              "aggregation_manipulation",
                                              "source_data_manipulation")
                                        for s in signals if s),
        "signals": signals,
    }
    out_path = abs_path("data/satellite/manipulation_tests.json")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(out, indent=2))
    print(f"[ok] manipulation tests → {out_path}")
    for k, v in out.items():
        if isinstance(v, dict) and v.get("status") == "ok":
            print(f"   {k}: {v.get('verdict', '-')}")
        elif isinstance(v, dict):
            print(f"   {k}: {v.get('status')}")


if __name__ == "__main__":
    main()
