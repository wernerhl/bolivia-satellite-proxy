"""Mixed-frequency dynamic factor model — paper §4.2.

Observation equation:  y_{i,t} = λ_i · f_t + e_{i,t}
State dynamics:        f_t     = φ_1 f_{t-1} + φ_2 f_{t-2} + η_t
Idiosyncratic AR(1):   e_{i,t} = ρ_i e_{i,t-1} + ε_{i,t}

Quarterly GDP enters through the Mariano-Murasawa (2003) aggregation
identity  ΔlogGDP^Q_t = (1/3)(f_t + 2f_{t-1} + 3f_{t-2} + 2f_{t-3} + f_{t-4}).

Implementation: statsmodels.tsa.statespace.dynamic_factor_mq.DynamicFactorMQ
handles the ragged-edge + mixed-frequency case out of the box. Estimation
is by EM (for speed on a few hundred months) or direct MLE.

Indicators (N = 7):
  monthly  — viirs_z, vnf_z, no2_z, cement_z, cndc_z, sin_z, aduana_z
  quarterly — ine_gdp_q_logdiff
"""
from __future__ import annotations

import json
import sys
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from _common import abs_path, load_env, paths  # noqa: E402

load_env()
warnings.filterwarnings("ignore", category=FutureWarning)


MONTHLY_KEYS = ("viirs_z", "vnf_z", "no2_z", "cement", "cndc", "sin", "aduana")


def _zscore(s: pd.Series) -> pd.Series:
    mu, sd = s.mean(), s.std(ddof=1)
    if sd and sd > 0:
        return (s - mu) / sd
    return s - mu


def assemble_panel() -> pd.DataFrame:
    p = paths()
    ci = pd.read_csv(abs_path(p["data"]["ci"]), parse_dates=["date"])
    ci = ci.sort_values("date").set_index("date")
    out = ci[["viirs_z", "vnf_z", "no2_z"]].copy()

    for key, src in [
        ("cement", p["data"]["official_cement"]),
        ("cndc", p["data"]["official_cndc"]),
        ("sin", p["data"]["official_sin"]),
        ("aduana", p["data"]["official_aduana"]),
    ]:
        path = abs_path(src)
        if path.exists():
            df = pd.read_csv(path, parse_dates=["date"]).set_index("date")
            val_col = next((c for c in df.columns if c != "date"), None)
            if val_col is not None:
                s = np.log(df[val_col].replace({0: np.nan}))
                # First-difference then z-score (activity change, standardized)
                out[key] = _zscore(s.diff())

    # Quarterly GDP as LHS via MM aggregation — statsmodels DFM-MQ handles it directly.
    gdp_path = abs_path("data/official/ine_gdp_quarterly.csv")
    if gdp_path.exists():
        gdp = pd.read_csv(gdp_path, parse_dates=["date"])
        gdp["log_growth"] = np.log(gdp["gdp_real"]).diff()
        gdp = gdp.set_index("date")[["log_growth"]]
        # Expand quarterly observations to month-end markers
        gdp = gdp.resample("MS").asfreq()
        out = out.join(gdp.rename(columns={"log_growth": "gdp_q"}))

    return out


def fit_dfm(panel: pd.DataFrame, k_factors: int = 1, factor_order: int = 2) -> dict:
    """Fit via statsmodels DynamicFactorMQ. Returns fitted factor + params."""
    from statsmodels.tsa.statespace.dynamic_factor_mq import DynamicFactorMQ

    monthly_cols = [c for c in panel.columns if c != "gdp_q"]
    endog_monthly = panel[monthly_cols].copy()
    # Drop columns that are entirely missing (official series not yet fetched).
    endog_monthly = endog_monthly.dropna(axis=1, how="all")
    # Drop rows where every remaining column is missing; the Kalman smoother
    # handles ragged edges and partial-NaN months fine.
    endog_monthly = endog_monthly.dropna(how="all")
    if endog_monthly.shape[1] == 0:
        return {"status": "no_indicators"}
    endog_quarterly = None
    if "gdp_q" in panel.columns:
        gdp = panel["gdp_q"].dropna()
        if len(gdp) >= 8:
            endog_quarterly = gdp.to_frame("gdp_q")

    try:
        model = DynamicFactorMQ(
            endog=endog_monthly,
            endog_quarterly=endog_quarterly,
            factors=k_factors,
            factor_orders=factor_order,
            idiosyncratic_ar1=True,
            standardize=True,
        )
        res = model.fit(disp=False)
        factor = res.factors.smoothed.iloc[:, 0]
        # Orient so positive factor = expansion. Correlate with the
        # weighted CI from build_ci; flip sign if negative.
        try:
            import pandas as _pd
            ci = _pd.read_csv(abs_path(paths()["data"]["ci"]), parse_dates=["date"])
            ci = ci.set_index("date")["ci"].dropna()
            overlap = _pd.concat([factor.rename("f"), ci], axis=1).dropna()
            if len(overlap) >= 12 and overlap["f"].corr(overlap["ci"]) < 0:
                factor = -factor
        except Exception:
            pass
        factor_zscore = _zscore(factor)
        return {
            "status": "ok",
            "n_obs": int(len(endog_monthly)),
            "log_likelihood": float(res.llf),
            "aic": float(res.aic),
            "factor_index": [d.strftime("%Y-%m-%d") for d in factor.index],
            "factor": factor.tolist(),
            "factor_z": factor_zscore.tolist(),
            "loadings": {c: float(res.params.get(f"loading.f1.{c}", np.nan))
                         for c in endog_monthly.columns},
        }
    except Exception as e:
        return {"status": "fit_failed", "error": str(e)[:200]}


def main() -> None:
    panel = assemble_panel()
    out_csv = abs_path("data/satellite/dfm_panel.csv")
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    panel.to_csv(out_csv)

    if panel.empty or len(panel) < 24:
        print(f"[..] DFM skipped: n={len(panel)} < 24")
        out = {"status": "insufficient_n", "n": int(len(panel))}
    else:
        out = fit_dfm(panel)
        if out["status"] == "ok":
            print(f"[ok] DFM: llf={out['log_likelihood']:.1f} n={out['n_obs']} "
                  f"factors=1 order=2")

    abs_path("data/satellite/dfm_result.json").write_text(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()
