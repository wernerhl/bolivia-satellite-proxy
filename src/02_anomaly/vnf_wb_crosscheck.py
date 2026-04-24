"""Cross-check: annualize VNF Chaco RH and regress against World Bank GGFR
Bolivia annual flare volume (BCM). Independent of YPFB — the WB series is
itself VNF-derived but vetted against Cedigaz country data, so consistency
(not independence) is what we're testing.

Writes vnf_wb_crosscheck.json with Pearson correlation, OLS elasticity of
log(BCM) on log(ΣRH), and per-year relative residual. A >25% deviation
between our Chaco aggregate and WB's Bolivia national total in two
consecutive years is flagged for methodology review.
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


def main() -> None:
    p = paths()
    vnf_path = abs_path(p["data"]["vnf_monthly"])
    wb_path = abs_path(p["data"]["wb_ggfr_country"])
    out_path = abs_path(p["data"]["vnf_wb_crosscheck"])
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if not vnf_path.exists() or not wb_path.exists():
        out_path.write_text(json.dumps({"status": "inputs_missing"}, indent=2))
        print(f"[warn] missing input → {out_path}")
        return

    vnf = pd.read_csv(vnf_path, parse_dates=["date"])
    wb = pd.read_csv(wb_path)
    if vnf.empty or wb.empty or "flare_volume_bcm" not in wb.columns:
        out_path.write_text(json.dumps({"status": "empty_input"}, indent=2))
        print(f"[warn] empty input → {out_path}")
        return

    # Annual ΣRH across all Chaco fields
    vnf["year"] = vnf["date"].dt.year
    annual_rh = vnf.groupby("year", as_index=False)["rh_mw_sum"].sum().rename(
        columns={"rh_mw_sum": "annual_rh"}
    )

    merged = annual_rh.merge(wb[["year", "flare_volume_bcm"]], on="year", how="inner")
    merged = merged[(merged["annual_rh"] > 0) & (merged["flare_volume_bcm"] > 0)].reset_index(drop=True)

    if len(merged) < 3:
        out_path.write_text(json.dumps({"status": "n_too_small", "n": int(len(merged))}, indent=2))
        print(f"[warn] n too small → {out_path}")
        return

    corr = float(np.corrcoef(np.log(merged["annual_rh"]), np.log(merged["flare_volume_bcm"]))[0, 1])

    X = sm.add_constant(np.log(merged["annual_rh"]))
    y = np.log(merged["flare_volume_bcm"])
    model = sm.OLS(y, X).fit(cov_type="HC1")
    beta = float(model.params.iloc[1])
    alpha = float(model.params.iloc[0])

    merged["pred_bcm"] = np.exp(alpha + beta * np.log(merged["annual_rh"]))
    merged["rel_residual"] = (merged["flare_volume_bcm"] - merged["pred_bcm"]) / merged["pred_bcm"]

    big = merged["rel_residual"].abs() > 0.25
    flag = bool((big & big.shift(1)).fillna(False).tail(5).any())

    result = {
        "status": "ok",
        "n_years": int(len(merged)),
        "corr_log_log": corr,
        "elasticity_beta": beta,
        "intercept_alpha": alpha,
        "per_year": [
            {
                "year": int(r["year"]),
                "annual_rh_mw": float(r["annual_rh"]),
                "wb_bcm": float(r["flare_volume_bcm"]),
                "pred_bcm": float(r["pred_bcm"]),
                "rel_residual": float(r["rel_residual"]),
            }
            for _, r in merged.iterrows()
        ],
        "flag_methodology_review": flag,
    }
    out_path.write_text(json.dumps(result, indent=2))
    print(f"[ok] crosscheck corr={corr:.3f}  β={beta:.3f}  n={len(merged)}  "
          f"flag={flag} → {out_path}")


if __name__ == "__main__":
    main()
