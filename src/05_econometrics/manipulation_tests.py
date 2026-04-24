"""Manipulation-detection suite — paper §4.3 (Track B rewrite).

Three single-country-identified tests. Order is ladder-of-evidence:
  1. Sectoral triangulation (physical layer) — LEAD
  2. November-2025 INE leadership discontinuity (statistical layer)
  3. External-forecaster residual falsification (consensus layer)

Each test writes its own block; a summary JSON combines all three and
reports which layers reject the no-manipulation null.
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


LEADERSHIP_BREAK = pd.Timestamp("2025-11-08")    # Paz inauguration
CRISIS_START = pd.Timestamp("2024-01-01")


# -------- TEST 1: sectoral triangulation -----------------------------
def test1_sectoral_triangulation() -> dict:
    """VNF ↔ YPFB ↔ INE hydrocarbon VA consistency test.

    If (VNF agrees with YPFB) AND (YPFB agrees with INE hydrocarbon VA),
    no manipulation is detected. If (VNF agrees with YPFB) but (YPFB does
    not agree with INE hydrocarbon VA), the national accounts aggregation
    is suspicious. If (VNF does not agree with YPFB), the source reporting
    itself is suspicious.

    Agreement threshold: log-log Pearson correlation ≥ 0.60 on pre-crisis
    sample (pre-2024), monthly panel.
    """
    p = paths()
    vnf_monthly = pd.read_csv(abs_path(p["data"]["vnf_monthly"]), parse_dates=["date"]) \
        if abs_path(p["data"]["vnf_monthly"]).exists() else pd.DataFrame()
    wb_annual = abs_path("data/official/wb_ggfr_bolivia_annual.csv")
    ypfb_path = abs_path(p["data"]["official_ypfb"])
    ine_hydro = abs_path("data/official/ine_hydrocarbon_va.csv")

    if not ypfb_path.exists() or not ine_hydro.exists():
        return {"status": "inputs_missing",
                "needs": ["ypfb_hydrocarbons.csv (date,gas_prod_mmm3d)",
                          "ine_hydrocarbon_va.csv (date,hydrocarbon_va)"]}

    ypfb = pd.read_csv(ypfb_path, parse_dates=["date"])
    ine = pd.read_csv(ine_hydro, parse_dates=["date"])

    # Preferred path: monthly VNF joined with monthly YPFB+INE.
    if not vnf_monthly.empty and "rh_mw_sum" in vnf_monthly.columns:
        flaring = vnf_monthly.groupby("date", as_index=False)["rh_mw_sum"].sum()
        df = flaring.merge(ypfb, on="date").merge(ine, on="date")
        df = df[(df["rh_mw_sum"] > 0) & (df["gas_prod_mmm3d"] > 0)
                & (df["hydrocarbon_va"] > 0)]
        flaring_col = "rh_mw_sum"
        granularity = "monthly"
    else:
        # Fallback: annual WB GGFR flare volume (BCM) as the physical-layer
        # proxy. Same interpretation; different unit (BCM instead of MW-nights).
        if not wb_annual.exists():
            return {"status": "inputs_missing",
                    "needs": ["wb_ggfr_bolivia_annual.csv or vnf_chaco_monthly.csv"]}
        wb = pd.read_csv(wb_annual)
        if "flare_volume_bcm" not in wb.columns:
            return {"status": "inputs_missing",
                    "needs": ["wb_ggfr_bolivia_annual flare_volume_bcm column"]}
        # Annualize YPFB and INE.
        ypfb_a = ypfb.assign(year=ypfb["date"].dt.year).groupby(
            "year", as_index=False)["gas_prod_mmm3d"].mean()
        ine_a = ine.assign(year=ine["date"].dt.year).groupby(
            "year", as_index=False)["hydrocarbon_va"].mean()
        df = wb[["year", "flare_volume_bcm"]].merge(ypfb_a, on="year").merge(
            ine_a, on="year")
        df = df[(df["flare_volume_bcm"] > 0) & (df["gas_prod_mmm3d"] > 0)
                & (df["hydrocarbon_va"] > 0)]
        df["date"] = pd.to_datetime(df["year"].astype(str) + "-01-01")
        flaring_col = "flare_volume_bcm"
        granularity = "annual_wb_ggfr"

    pre = df[df["date"] < CRISIS_START]
    if len(pre) < (24 if granularity == "monthly" else 6):
        return {"status": "insufficient_pre_crisis_n", "n": int(len(pre)),
                "granularity": granularity}

    r_vnf_ypfb = float(np.corrcoef(np.log(pre[flaring_col]),
                                   np.log(pre["gas_prod_mmm3d"]))[0, 1])
    r_ypfb_ine = float(np.corrcoef(np.log(pre["gas_prod_mmm3d"]),
                                    np.log(pre["hydrocarbon_va"]))[0, 1])

    a_ok = r_vnf_ypfb >= 0.60
    b_ok = r_ypfb_ine >= 0.60

    # Special case for dry-gas regimes (Bolivia, Turkmenistan): flaring is
    # operational rather than volumetric, so low corr(flaring, production)
    # is not evidence of source-layer manipulation — it is the Track C
    # finding that flaring is a capacity-utilization proxy, not a
    # production proxy. A high corr(production, VA) in this case is
    # consistent with honest reporting. Flag distinctly.
    if not a_ok and b_ok and abs(r_vnf_ypfb) < 0.30:
        verdict = "dry_gas_flaring_not_volumetric_no_manipulation_signal"
    elif a_ok and b_ok:
        verdict = "no_manipulation_detected"
    elif a_ok and not b_ok:
        verdict = "aggregation_layer_suspect"
    elif not a_ok and b_ok:
        verdict = "source_layer_suspect"
    else:
        verdict = "both_layers_suspect"

    # Crisis-window residual: compute log-log regression coefficients on
    # pre-crisis and project onto crisis window; report mean residual.
    X_pre = sm.add_constant(np.log(pre["gas_prod_mmm3d"]))
    y_pre = np.log(pre["hydrocarbon_va"])
    fit = sm.OLS(y_pre, X_pre).fit()
    crisis = df[df["date"] >= CRISIS_START]
    crisis_resid = None
    if len(crisis) >= 1:
        pred = fit.params.iloc[0] + fit.params.iloc[1] * np.log(crisis["gas_prod_mmm3d"])
        resid = np.log(crisis["hydrocarbon_va"]) - pred
        crisis_resid = float(resid.mean())

    return {
        "status": "ok",
        "granularity": granularity,
        "pre_crisis_n": int(len(pre)),
        "vnf_ypfb_corr": r_vnf_ypfb,
        "ypfb_ine_corr": r_ypfb_ine,
        "verdict": verdict,
        "crisis_window_mean_residual_log_hydro_va": crisis_resid,
        "identifying_assumption":
            "Flaring is a physical, tamper-evident observation of field operations. "
            "If the three series diverge, the divergence localizes to either source "
            "reporting or national-accounts aggregation.",
    }


# -------- TEST 2: Nov-2025 INE leadership discontinuity --------------
def test2_leadership_discontinuity() -> dict:
    """Δlog(IGAE_t) = α + β Δlog(CI_t) + δ D_post + γ [Δlog(CI_t)·D_post]
                    + ψ·X_t + ε_t,   H0: γ = 0.

    Controls X_t: parallel premium, reserve change, month dummies,
    precipitation anomaly (if present). Uses quarterly GDP when IGAE is
    shorter than 12 months post-break.
    """
    p = paths()
    ci = pd.read_csv(abs_path(p["data"]["ci"]), parse_dates=["date"])
    ci = ci.dropna(subset=["ci"]).sort_values("date").set_index("date")

    igae_path = abs_path(p["data"]["official_igae"])
    gdp_path = abs_path("data/official/ine_gdp_quarterly.csv")
    if not igae_path.exists() and not gdp_path.exists():
        return {"status": "inputs_missing",
                "needs": ["ine_igae.csv OR ine_gdp_quarterly.csv"]}

    if igae_path.exists():
        y = pd.read_csv(igae_path, parse_dates=["date"]).set_index("date")["igae"]
        y = np.log(y).diff().rename("dy")
        ci_aligned = ci["ci"].diff().rename("dx")
        frequency = "monthly_igae"
    else:
        q = pd.read_csv(gdp_path, parse_dates=["date"]).set_index("date")["gdp_real"]
        y = np.log(q).diff().rename("dy")
        # Project CI to quarterly
        ci_q = ci["ci"].resample("QE").mean().diff().rename("dx")
        ci_aligned = ci_q
        frequency = "quarterly_gdp"

    df = pd.concat([y, ci_aligned], axis=1).dropna()
    if len(df) < 12:
        return {"status": "insufficient_n", "n": int(len(df)), "frequency": frequency}

    df["D_post"] = (df.index >= LEADERSHIP_BREAK).astype(int)
    df["dx_x_D"] = df["dx"] * df["D_post"]

    # Optional control: dollar premium if present
    prem_path = abs_path("data/official/dollar_premium.csv")
    if prem_path.exists():
        prem = pd.read_csv(prem_path, parse_dates=["date"]).set_index("date")
        if "dollar_premium" in prem.columns:
            pm = prem["dollar_premium"]
            if frequency == "quarterly_gdp":
                pm = pm.resample("QE").mean()
            df = df.join(pm.rename("dollar_premium"), how="left")

    X_cols = ["dx", "D_post", "dx_x_D"]
    if "dollar_premium" in df.columns:
        X_cols.append("dollar_premium")
    X = sm.add_constant(df[X_cols])
    model = sm.OLS(df["dy"], X).fit(cov_type="HAC", cov_kwds={"maxlags": 3})

    n_post = int(df["D_post"].sum())
    return {
        "status": "ok",
        "frequency": frequency,
        "n": int(len(df)),
        "n_post_break": n_post,
        "beta_pre": float(model.params["dx"]),
        "beta_se_pre": float(model.bse["dx"]),
        "gamma_interaction": float(model.params["dx_x_D"]),
        "gamma_se": float(model.bse["dx_x_D"]),
        "gamma_pvalue": float(model.pvalues["dx_x_D"]),
        "delta_intercept_shift": float(model.params["D_post"]),
        "verdict": (
            "martinez_signal" if (model.params["dx_x_D"] > 0
                                  and model.pvalues["dx_x_D"] < 0.10) else
            "no_signal"
        ),
        "preliminary":
            ("Bolivia's monthly IGAE starts March 2026; post-break sample has "
             f"{n_post} observations as of this vintage. Interpret with caution; "
             "revisit at R&R.") if frequency == "monthly_igae" else None,
        "identifying_assumption":
            "Hydrocarbon fundamentals, reserves trajectory, and the parallel-rate "
            "premium are continuous across November 2025. A discontinuity in the "
            "satellite-to-GDP elasticity is therefore attributable to statistical "
            "production, not fundamentals.",
    }


# -------- TEST 3: external-forecaster residual ----------------------
def test3_external_forecaster_residual() -> dict:
    """Regress CI on INE growth and on external forecaster growth separately;
    compare the sign and magnitude of residuals during 2024-2025.

    Expected consensus: external forecasters (IMF, World Bank, Oxford, S&P)
    forecasts for 2025/2026 are more negative than INE. If INE smoothed
    upward during the acute crisis, satellite-implied growth should be
    systematically closer to external forecasters than to INE in 2024-2025.
    """
    p = paths()
    ci = pd.read_csv(abs_path(p["data"]["ci"]), parse_dates=["date"])
    ci = ci.dropna(subset=["ci"]).sort_values("date").set_index("date")

    gdp_path = abs_path("data/official/ine_gdp_quarterly.csv")
    ext_path = abs_path("data/official/external_forecasters.csv")
    if not gdp_path.exists() or not ext_path.exists():
        return {"status": "inputs_missing",
                "needs": ["ine_gdp_quarterly.csv (date,gdp_real)",
                          "external_forecasters.csv (year,imf,wb,oxford,snp)"]}

    gdp = pd.read_csv(gdp_path, parse_dates=["date"]).set_index("date")
    # INE growth (YoY) from quarterly
    ine_g = np.log(gdp["gdp_real"]).diff(4).rename("ine_g")
    ext = pd.read_csv(ext_path)  # year, imf, wb, oxford, snp (percent growth)
    ext["consensus"] = ext[["imf", "wb", "oxford", "snp"]].mean(axis=1) / 100.0

    # Resample CI to annual (mean)
    ci_a = ci["ci"].resample("YE").mean()
    # Annualize INE growth
    ine_a = ine_g.resample("YE").mean()
    ext_a = ext.set_index(pd.to_datetime(ext["year"].astype(str) + "-12-31"))["consensus"]

    df = pd.concat([ci_a.rename("ci"), ine_a, ext_a], axis=1).dropna()
    if len(df) < 6:
        return {"status": "insufficient_n", "n": int(len(df))}

    crisis_years = df.index.year >= 2024
    resid_ine = df["ci"] - df["ine_g"]
    resid_ext = df["ci"] - df["consensus"]
    return {
        "status": "ok",
        "n": int(len(df)),
        "ine_residual_crisis_mean": float(resid_ine[crisis_years].mean())
            if crisis_years.any() else None,
        "ext_residual_crisis_mean": float(resid_ext[crisis_years].mean())
            if crisis_years.any() else None,
        "verdict": (
            "consistent_with_external_consensus"
            if (crisis_years.any()
                and abs(resid_ext[crisis_years].mean())
                    < abs(resid_ine[crisis_years].mean()))
            else "consistent_with_ine"
        ),
        "identifying_assumption":
            "External forecasters and the satellite composite draw from the "
            "same macro-data universe (reserves, prices, trade) but produce "
            "independent numbers. A satellite residual smaller against the "
            "external consensus than against INE places the satellite "
            "evidence closer to the external-consensus reading of the "
            "recession.",
    }


def main() -> None:
    out = {
        "test1_sectoral_triangulation": test1_sectoral_triangulation(),
        "test2_leadership_discontinuity": test2_leadership_discontinuity(),
        "test3_external_forecaster_residual": test3_external_forecaster_residual(),
    }
    signals = [r.get("verdict") for r in out.values() if isinstance(r, dict)]
    layer_rejections = {
        "physical": out["test1_sectoral_triangulation"].get("verdict") in (
            "aggregation_layer_suspect", "source_layer_suspect", "both_layers_suspect"),
        "statistical": out["test2_leadership_discontinuity"].get("verdict") == "martinez_signal",
        "consensus": out["test3_external_forecaster_residual"].get("verdict")
                     == "consistent_with_external_consensus",
    }
    out["ladder_summary"] = {
        "layer_rejections": layer_rejections,
        "n_layers_rejecting": sum(1 for v in layer_rejections.values() if v),
        "strong_claim": all(layer_rejections.values()),
        "publishable_claim": any(layer_rejections.values()),
    }
    out_path = abs_path("data/satellite/manipulation_tests.json")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(out, indent=2))
    print(f"[ok] manipulation tests → {out_path}")
    for k, v in out.items():
        if isinstance(v, dict) and "status" in v:
            print(f"   {k}: {v.get('status')} / {v.get('verdict', '-')}")


if __name__ == "__main__":
    main()
