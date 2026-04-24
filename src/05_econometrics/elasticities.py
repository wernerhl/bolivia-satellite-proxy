"""Single-series elasticities — paper §4.1, equations (6), (7), (8).

  (6)  Δlog GDP_{d,t} = α_d + β_VIIRS · Δlog SOL_{d,t} + γ_t + ε_{d,t}
       department × year panel, two-way clustered SEs.
  (7)  Δlog GasProd_{f,t} = α_f + β_VNF · Δlog RH_{f,t} + γ_t + ε_{f,t}
       field × month panel, two-way clustered SEs.
  (8)  Δlog NO2_{m,t} = α_m + β_NO2 · Δlog FuelSales_{m,t}
                         + δ · D^post-Dec25_t + γ_t + ε_{m,t}
       metro × month panel, two-way clustered SEs.

Each requires an external series that the harness may or may not have
fetched yet. On missing inputs we write a JSON reporting status;
downstream table scripts read the JSON and render NA rows.

Input files:
  * Eq (6):  data/official/ine_gdp_dept.csv        (year, department, gdp_usd)
             data/satellite/viirs_sol_dept_annual.csv
                                                    (year, department, sol)
  * Eq (7):  data/satellite/vnf_chaco_monthly.csv   (date, field, rh_mw_sum)
             data/official/ypfb_field_month.csv    (date, field, gas_prod_mmm3d)
  * Eq (8):  data/satellite/s5p_no2_monthly.csv    (date, roi, no2_tropos_col_mol_m2)
             data/official/ypfb_fuel_sales_metro.csv
                                                    (date, roi, fuel_sales)

Outputs:
  data/satellite/elasticity_viirs.json
  data/satellite/elasticity_vnf.json
  data/satellite/elasticity_no2.json
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


def _twoway_cluster(model, df: pd.DataFrame, cluster_a: str, cluster_b: str):
    """Cameron-Gelbach-Miller two-way-clustered SE on a statsmodels fit."""
    import statsmodels.formula.api as smf
    # statsmodels OLS via formula lets us re-fit with cov_kwds
    return model  # placeholder; we compute SEs manually below


def _cgm_se(X: np.ndarray, y: np.ndarray, resid: np.ndarray,
            a: np.ndarray, b: np.ndarray) -> np.ndarray:
    """Cameron-Gelbach-Miller (2011) two-way-clustered SE."""
    n, k = X.shape
    XtX_inv = np.linalg.inv(X.T @ X)

    def meat(g):
        groups = np.unique(g)
        M = np.zeros((k, k))
        for gi in groups:
            mask = g == gi
            Xg = X[mask]
            ug = resid[mask]
            s = Xg.T @ ug
            M += np.outer(s, s)
        return M

    M_a = meat(a)
    M_b = meat(b)
    ab = a.astype(str) + "|" + b.astype(str)
    M_ab = meat(np.array(ab))
    V = XtX_inv @ (M_a + M_b - M_ab) @ XtX_inv
    return np.sqrt(np.diag(V))


def _fit_diff_in_diff_fe(df: pd.DataFrame, ycol: str, xcol: str,
                          unit: str, time: str,
                          extra: list[str] | None = None) -> dict:
    """Panel FE on first differences with two-way-clustered SEs.

    We absorb unit FE by including the first-difference Δ (which removes
    time-invariant unit effects) and add year dummies for γ_t. Final SE
    uses Cameron-Gelbach-Miller clustering on (unit, time).
    """
    extra = extra or []
    df = df.sort_values([unit, time]).copy()
    df[f"dy"] = df.groupby(unit)[ycol].diff()
    df[f"dx"] = df.groupby(unit)[xcol].diff()
    for e in extra:
        df[f"d_{e}"] = df.groupby(unit)[e].diff() if df[e].dtype != bool else df[e].astype(int)
    df = df.dropna(subset=["dy", "dx"])

    if len(df) < 10:
        return {"status": "insufficient_n", "n": int(len(df))}

    # Year dummies
    df[time] = df[time].astype(str)
    yr_dummies = pd.get_dummies(df[time], prefix="t", drop_first=True).astype(float)
    X_cols = ["dx"] + [f"d_{e}" for e in extra] + list(yr_dummies.columns)
    X = np.column_stack([np.ones(len(df))] + [df[c].to_numpy() for c in ["dx"] + [f"d_{e}" for e in extra]] + [yr_dummies[c].to_numpy() for c in yr_dummies.columns])
    y = df["dy"].to_numpy()

    beta = np.linalg.solve(X.T @ X, X.T @ y)
    resid = y - X @ beta
    se = _cgm_se(X, y, resid,
                 a=df[unit].to_numpy(), b=df[time].to_numpy())
    t = beta / se
    # p-values from t; with large n, Normal approximation is fine
    from scipy.stats import norm
    pval = 2 * (1 - norm.cdf(np.abs(t)))

    return {
        "status": "ok",
        "n": int(len(df)),
        "beta": float(beta[1]),
        "se": float(se[1]),
        "t": float(t[1]),
        "p": float(pval[1]),
        "n_units": int(df[unit].nunique()),
        "n_periods": int(df[time].nunique()),
    }


def viirs_elasticity() -> dict:
    """Eq. (1) — Bolivia-specific HSW-style elasticity on the departmental
    panel. Accepts either gdp_real (INE 1990-base) or gdp_usd."""
    p = paths()
    gdp = abs_path("data/official/ine_gdp_dept.csv")
    sol = abs_path("data/satellite/viirs_sol_dept_annual.csv")
    if not gdp.exists() or not sol.exists():
        return {"status": "inputs_missing",
                "needs": ["data/official/ine_gdp_dept.csv (year,department,gdp_real|gdp_usd)",
                          "data/satellite/viirs_sol_dept_annual.csv (year,department,sol)"]}
    a = pd.read_csv(gdp); b = pd.read_csv(sol)
    df = a.merge(b, on=["year", "department"])
    gdp_col = "gdp_real" if "gdp_real" in df.columns else "gdp_usd"
    df = df[(df[gdp_col] > 0) & (df["sol"] > 0)]
    df["log_gdp"] = np.log(df[gdp_col])
    df["log_sol"] = np.log(df["sol"])
    return _fit_diff_in_diff_fe(df, "log_gdp", "log_sol", "department", "year")


def vnf_elasticity() -> dict:
    p = paths()
    ypfb = abs_path("data/official/ypfb_field_month.csv")
    vnf = abs_path(p["data"]["vnf_monthly"])
    if not ypfb.exists() or not vnf.exists():
        return {"status": "inputs_missing",
                "needs": ["data/official/ypfb_field_month.csv (date,field,gas_prod_mmm3d)"]}
    a = pd.read_csv(ypfb, parse_dates=["date"])
    b = pd.read_csv(vnf, parse_dates=["date"])
    df = a.merge(b, on=["date", "field"])
    df = df[(df["gas_prod_mmm3d"] > 0) & (df["rh_mw_sum"] > 0)]
    df["log_gas"] = np.log(df["gas_prod_mmm3d"])
    df["log_rh"] = np.log(df["rh_mw_sum"])
    df["month_idx"] = df["date"].dt.to_period("M").astype(str)
    return _fit_diff_in_diff_fe(df, "log_gas", "log_rh", "field", "month_idx")


def no2_elasticity() -> dict:
    p = paths()
    fuel = abs_path("data/official/ypfb_fuel_sales_metro.csv")
    no2 = abs_path(p["data"]["s5p_monthly"])
    if not fuel.exists() or not no2.exists():
        return {"status": "inputs_missing",
                "needs": ["data/official/ypfb_fuel_sales_metro.csv (date,roi,fuel_sales)"]}
    a = pd.read_csv(fuel, parse_dates=["date"])
    b = pd.read_csv(no2, parse_dates=["date"])
    df = a.merge(b, on=["date", "roi"]).dropna(subset=["no2_tropos_col_mol_m2", "fuel_sales"])
    df = df[(df["fuel_sales"] > 0) & (df["no2_tropos_col_mol_m2"] > 0)]
    df["log_no2"] = np.log(df["no2_tropos_col_mol_m2"])
    df["log_fuel"] = np.log(df["fuel_sales"])
    df["post_break"] = (df["date"] >= pd.Timestamp("2025-12-01")).astype(int)
    df["month_idx"] = df["date"].dt.to_period("M").astype(str)
    return _fit_diff_in_diff_fe(df, "log_no2", "log_fuel", "roi", "month_idx",
                                 extra=["post_break"])


def main() -> None:
    p = paths()
    results = {
        "viirs": viirs_elasticity(),
        "vnf": vnf_elasticity(),
        "no2": no2_elasticity(),
    }
    out_dir = abs_path("data/satellite")
    out_dir.mkdir(parents=True, exist_ok=True)
    for name, res in results.items():
        (out_dir / f"elasticity_{name}.json").write_text(json.dumps(res, indent=2))
        status = res.get("status")
        if status == "ok":
            print(f"[ok] {name}: β={res['beta']:+.3f} (se {res['se']:.3f}) n={res['n']}")
        else:
            print(f"[..] {name}: {status}")


if __name__ == "__main__":
    main()
