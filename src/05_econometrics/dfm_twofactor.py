"""Track D — two-factor DFM (urban + extractive) with agricultural NDVI as a
third factor. Fallback to the Lewis-Mertens-Stock (2022) pre-specified
weighted-composite spec if the state-space EM fails.

Urban factor loads:     viirs_z, no2_z, cement, cndc, sin, aduana
Extractive factor loads: vnf_z, ypfb_gas, hydrocarbon_electricity
Agricultural factor:    ndvi_z

Sign identification: after fit, flip each factor so its loading on its
lead indicator (viirs_z for urban, vnf_z for extractive, ndvi_z for ag)
is positive.
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


URBAN_GROUP = ["viirs_z", "no2_z", "cement", "cndc", "sin", "aduana"]
EXTRACTIVE_GROUP = ["vnf_z", "ypfb_gas", "hydrocarbon_electricity"]
AG_GROUP = ["ndvi_z"]

LEAD = {"urban": "viirs_z", "extractive": "vnf_z", "agricultural": "ndvi_z"}

# GDP sectoral weights for the fallback pre-specified composite.
GDP_WEIGHTS = {"urban": 0.55, "extractive": 0.20, "agricultural": 0.14, "residual": 0.11}


def _zscore(s: pd.Series) -> pd.Series:
    mu, sd = s.mean(), s.std(ddof=1)
    return (s - mu) / (sd if sd and sd > 0 else 1.0)


def assemble_panel() -> pd.DataFrame:
    p = paths()
    ci = pd.read_csv(abs_path(p["data"]["ci"]), parse_dates=["date"])
    ci = ci.sort_values("date").set_index("date")
    out = ci[[c for c in ("viirs_z", "vnf_z", "no2_z", "ndvi_z") if c in ci.columns]].copy()

    def _load(key: str, col_name: str, is_flow: bool = True) -> None:
        src = abs_path(p["data"].get(key, ""))
        if src.exists():
            df = pd.read_csv(src, parse_dates=["date"]).set_index("date")
            val_col = next((c for c in df.columns if c != "date"), None)
            if val_col is not None:
                s = np.log(df[val_col].replace({0: np.nan}))
                out[col_name] = _zscore(s.diff()) if is_flow else _zscore(s)

    _load("official_cement", "cement")
    _load("official_cndc", "cndc")
    _load("official_sin", "sin")
    _load("official_aduana", "aduana")
    # Extractive side supplementary series — usually absent; left as NaN.
    ypfb_path = abs_path(p["data"]["official_ypfb"])
    if ypfb_path.exists():
        y = pd.read_csv(ypfb_path, parse_dates=["date"]).set_index("date")
        if "gas_prod_mmm3d" in y.columns:
            out["ypfb_gas"] = _zscore(np.log(y["gas_prod_mmm3d"]).diff())
    return out


def fit_two_factor(panel: pd.DataFrame) -> dict:
    """DynamicFactorMQ with factor-multiplicities (two-factor block)."""
    from statsmodels.tsa.statespace.dynamic_factor_mq import DynamicFactorMQ

    endog = panel.dropna(how="all", axis=1).dropna(how="all")
    if endog.shape[1] < 3:
        return {"status": "too_few_indicators", "n_cols": int(endog.shape[1])}

    # Build factor block mapping: each indicator belongs to a named block.
    blocks = {"urban": [], "extractive": [], "agricultural": []}
    for c in endog.columns:
        if c in URBAN_GROUP:
            blocks["urban"].append(c)
        elif c in EXTRACTIVE_GROUP:
            blocks["extractive"].append(c)
        elif c in AG_GROUP:
            blocks["agricultural"].append(c)

    active_blocks = {k: v for k, v in blocks.items() if v}
    if not active_blocks:
        return {"status": "no_blocks"}

    # statsmodels DynamicFactorMQ accepts a dict of factor->variables via the
    # `factor_multiplicities` argument but cleanest way is to fit separate
    # single-factor DFMs per block and concatenate; avoids the fragile EM.
    factors: dict[str, pd.Series] = {}
    loadings: dict[str, dict[str, float]] = {}
    for block_name, cols in active_blocks.items():
        block_df = endog[cols].dropna(how="all")
        if block_df.shape[0] < 24 or block_df.shape[1] == 0:
            continue
        try:
            model = DynamicFactorMQ(
                endog=block_df, factors=1, factor_orders=2,
                idiosyncratic_ar1=True, standardize=True,
            )
            res = model.fit(disp=False)
            f = res.factors.smoothed.iloc[:, 0]
            # Sign identification: positive loading on the lead indicator
            lead_col = LEAD[block_name] if LEAD[block_name] in cols else cols[0]
            try:
                # Align factor with the lead indicator on overlap
                overlap = pd.concat([f.rename("f"), block_df[lead_col]], axis=1).dropna()
                if len(overlap) >= 12 and overlap["f"].corr(overlap[lead_col]) < 0:
                    f = -f
            except Exception:
                pass
            factors[block_name] = f
            loadings[block_name] = {c: float(res.params.get(f"loading.f1.{c}", np.nan))
                                     for c in cols}
        except Exception as e:
            loadings[block_name] = {"_error": str(e)[:160]}

    if not factors:
        return {"status": "all_blocks_failed", "loadings": loadings}

    # Align on common index, z-score each factor, build composite.
    fac_df = pd.concat([f.rename(k) for k, f in factors.items()], axis=1)
    fac_z = fac_df.apply(_zscore)

    # Composite weights: use GDP shares, with residual redistributed to urban.
    gw = GDP_WEIGHTS.copy()
    redistributed = gw.pop("residual", 0) / 3
    for k in gw:
        gw[k] = gw[k] + redistributed
    # Re-normalize to blocks we actually have.
    present = {k: gw[k] for k in fac_z.columns if k in gw}
    ws = sum(present.values())
    present = {k: v / ws for k, v in present.items()} if ws > 0 else present

    composite = sum(present.get(k, 0) * fac_z[k].fillna(0) for k in fac_z.columns)

    return {
        "status": "ok",
        "method": "two_factor_dfm_blocks",
        "n_obs": int(len(fac_df)),
        "blocks": list(factors.keys()),
        "weights": present,
        "factor_index": [d.strftime("%Y-%m-%d") for d in fac_df.index],
        "factors": {k: fac_z[k].tolist() for k in fac_z.columns},
        "composite_z": composite.tolist(),
        "loadings": loadings,
    }


def weighted_composite_fallback(panel: pd.DataFrame) -> dict:
    """Lewis-Mertens-Stock (2022) style: pre-specified weighted average of
    block composites, no estimation beyond standardization."""
    endog = panel.dropna(how="all", axis=1).dropna(how="all")

    def block_mean(cols: list[str]) -> pd.Series:
        sub = endog[[c for c in cols if c in endog.columns]]
        return sub.mean(axis=1) if sub.shape[1] > 0 else pd.Series(dtype=float)

    urban = block_mean(URBAN_GROUP)
    extractive = block_mean(EXTRACTIVE_GROUP)
    ag = block_mean(AG_GROUP)

    gw = GDP_WEIGHTS
    present = {}
    if not urban.empty:
        present["urban"] = gw["urban"]
    if not extractive.empty:
        present["extractive"] = gw["extractive"]
    if not ag.empty:
        present["agricultural"] = gw["agricultural"]
    ws = sum(present.values())
    present = {k: v / ws for k, v in present.items()} if ws > 0 else {}

    idx = pd.concat([urban, extractive, ag], axis=1).index
    composite = pd.Series(0, index=idx, dtype=float)
    block_series = {"urban": urban, "extractive": extractive, "agricultural": ag}
    for k, w in present.items():
        composite = composite + w * block_series[k].reindex(idx).fillna(0)

    return {
        "status": "ok",
        "method": "lmst_weighted_composite",
        "n_obs": int(len(idx)),
        "weights": present,
        "factor_index": [d.strftime("%Y-%m-%d") for d in idx],
        "composite_z": composite.tolist(),
    }


def main() -> None:
    panel = assemble_panel()
    out_csv = abs_path("data/satellite/dfm_twofactor_panel.csv")
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    panel.to_csv(out_csv)

    out_path = abs_path("data/satellite/dfm_twofactor_result.json")
    if panel.empty or len(panel) < 24:
        out_path.write_text(json.dumps({"status": "insufficient_n", "n": int(len(panel))}, indent=2))
        print(f"[warn] n too small: {len(panel)}")
        return

    result = fit_two_factor(panel)
    if result.get("status") != "ok":
        print(f"[..] two-factor DFM: {result.get('status')} — falling back to LMST composite")
        result = weighted_composite_fallback(panel)
    out_path.write_text(json.dumps(result, indent=2))
    print(f"[ok] {result.get('method')}: blocks={result.get('blocks') or list(result.get('weights', {}))}"
          f" weights={result.get('weights')}")


if __name__ == "__main__":
    main()
