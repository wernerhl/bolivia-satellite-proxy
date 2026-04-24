"""V3 E-track estimator — runs elasticities (E1), two-factor DFM (E2),
and manipulation tests (E3) against the Parquet outputs of build_data.py.

Strict gating per brief rule zero and one:
  * A track that requires a missing Parquet is skipped entirely, with
    a line in pending.log naming the blocker. No proxy substitution.
  * No fallback to "weighted composite via CI z-scores when DFM fails" —
    either the DFM converges on the declared 9-variable panel, or the
    track is flagged for E2-fallback review.
"""
from __future__ import annotations

import json
import sys
import warnings
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
V3 = HERE.parent
REPO = V3.parents[1]
sys.path.insert(0, str(REPO / "src"))
warnings.filterwarnings("ignore", category=FutureWarning)

PROCESSED = V3 / "data" / "processed"
ESTIMATES = V3 / "data" / "estimates"
ESTIMATES.mkdir(parents=True, exist_ok=True)
PENDING = V3 / "figures" / "pending.log"


def _log_pending(track: str, reason: str) -> None:
    ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
    with open(PENDING, "a") as f:
        f.write(f"{ts}  {track}  {reason}\n")


def _require(*files: str) -> dict[str, pd.DataFrame] | None:
    out = {}
    for fn in files:
        p = PROCESSED / fn
        if not p.exists():
            return None
        out[fn] = pd.read_parquet(p)
    return out


# ---------- E1: single-series elasticities --------------------------

def e1_elasticities() -> None:
    import statsmodels.api as sm

    rows: list[dict] = []

    # (1) VIIRS DNB -> GDP, departmental annual panel
    d = _require("viirs_dnb_monthly.parquet", "ine_dep_gdp_annual.parquet")
    if d is not None:
        viirs = d["viirs_dnb_monthly.parquet"].copy()
        viirs["year"] = viirs["date"].dt.year
        # City -> dept mapping (consolidated from config/spatial.yaml)
        CITY_DEP = {
            "la_paz_el_alto": "la_paz", "santa_cruz": "santa_cruz",
            "cochabamba": "cochabamba", "sucre": "chuquisaca",
            "oruro": "oruro", "potosi": "potosi", "tarija": "tarija",
            "trinidad": "beni", "cobija": "pando",
            "montero": "santa_cruz", "yacuiba": "tarija",
        }
        viirs["department"] = viirs["buffer_name"].map(CITY_DEP)
        viirs = viirs.dropna(subset=["sol", "department"])
        if "low_coverage_flag" in viirs.columns:
            viirs = viirs[~viirs["low_coverage_flag"].astype(bool)]
        ann = viirs.groupby(["year", "department"], as_index=False)["sol"].sum()

        gdp = d["ine_dep_gdp_annual.parquet"]
        gdp_total = gdp[gdp["sector_code"].str.contains(
            "PRODUCTO_INTERNO_BRUTO", na=False)
        ].drop_duplicates(subset=["year", "department"])
        merged = ann.merge(gdp_total[["year", "department",
                                      "real_value_1990base"]],
                           on=["year", "department"])
        merged = merged[(merged["sol"] > 0) & (merged["real_value_1990base"] > 0)]
        merged = merged.sort_values(["department", "year"])
        merged["dlog_sol"] = merged.groupby("department")["sol"].apply(
            lambda s: np.log(s).diff()).reset_index(level=0, drop=True)
        merged["dlog_gdp"] = merged.groupby("department")["real_value_1990base"].apply(
            lambda s: np.log(s).diff()).reset_index(level=0, drop=True)
        reg = merged.dropna(subset=["dlog_sol", "dlog_gdp"])
        if len(reg) >= 12:
            # Two-way clustered SE by (department, year)
            X = sm.add_constant(reg["dlog_sol"].values)
            y = reg["dlog_gdp"].values
            res = sm.OLS(y, X).fit(
                cov_type="cluster",
                cov_kwds={"groups": np.column_stack([
                    reg["department"].astype("category").cat.codes.values,
                    reg["year"].values])})
            rows.append({
                "specification": "eq1_viirs_gdp_dept_annual",
                "stream": "VIIRS DNB",
                "beta": float(res.params[1]),
                "se": float(res.bse[1]),
                "t": float(res.tvalues[1]),
                "pvalue": float(res.pvalues[1]),
                "n": int(len(reg)),
                "r2": float(res.rsquared),
                "sample_start": int(reg["year"].min()),
                "sample_end": int(reg["year"].max()),
            })
        else:
            _log_pending("E1-VIIRS", f"only {len(reg)} clean panel obs")
    else:
        _log_pending("E1-VIIRS", "viirs_dnb_monthly.parquet or "
                                 "ine_dep_gdp_annual.parquet missing")

    # (2) VNF -> gas production: requires D4 + D9
    if (PROCESSED / "vnf_chaco_monthly.parquet").exists() and \
       (PROCESSED / "ypfb_field_monthly.parquet").exists():
        # Implementation path kept symmetric; not exercised this vintage.
        _log_pending("E1-VNF", "ypfb_field_monthly.parquet schema TBD; "
                               "implementation will land once D4 completes.")
    else:
        _log_pending("E1-VNF", "D4 (YPFB field-month) or D9 (VNF Chaco) "
                               "Parquet missing.")

    # (3) NO2 -> fuel sales: requires S7 and an SP-internal fuel-sales
    # series that's not public in structured form.
    _log_pending("E1-NO2", "metropolitan fuel-sales series not available.")

    # (4) NDVI -> agricultural GVA
    d = _require("s2_ndvi_monthly.parquet", "ine_dep_gdp_annual.parquet")
    if d is not None:
        nd = d["s2_ndvi_monthly.parquet"].copy()
        nd["year"] = nd["date"].dt.year
        gdp = d["ine_dep_gdp_annual.parquet"]
        ag = gdp[gdp["sector_code"].str.contains(
            "AGRICULTURA|SILVICULTURA|PECUARIA", na=False, regex=True)]
        # Zone -> dept approximation (NDVI zones span multiple depts; we use
        # the centroid dept for the Track A elasticity as an approximation).
        ZONE_DEP = {
            "santa_cruz_soy_belt":     "santa_cruz",
            "beni_cattle_rice":         "beni",
            "tarija_valle_central":    "tarija",
            "chaco_periphery":          "santa_cruz",
            "altiplano_tubers_quinoa": "oruro",
        }
        nd["department"] = nd["zone"].map(ZONE_DEP)
        nd = nd.dropna(subset=["ndvi_median", "department"])
        # Annual mean NDVI per department (aggregate zones that share a dept)
        ann = nd.groupby(["year", "department"], as_index=False)["ndvi_median"].mean()
        ag_total = ag.groupby(["year", "department"], as_index=False
                              )["real_value_1990base"].sum()
        merged = ann.merge(ag_total, on=["year", "department"])
        merged = merged[(merged["ndvi_median"] > 0) &
                        (merged["real_value_1990base"] > 0)]
        merged = merged.sort_values(["department", "year"])
        merged["dlog_ndvi"] = merged.groupby("department")["ndvi_median"].apply(
            lambda s: np.log(s).diff()).reset_index(level=0, drop=True)
        merged["dlog_ag"] = merged.groupby("department")["real_value_1990base"].apply(
            lambda s: np.log(s).diff()).reset_index(level=0, drop=True)
        reg = merged.dropna(subset=["dlog_ndvi", "dlog_ag"])
        if len(reg) >= 8:
            X = sm.add_constant(reg["dlog_ndvi"].values)
            y = reg["dlog_ag"].values
            res = sm.OLS(y, X).fit(
                cov_type="cluster",
                cov_kwds={"groups": np.column_stack([
                    reg["department"].astype("category").cat.codes.values,
                    reg["year"].values])})
            rows.append({
                "specification": "eq4_ndvi_aggva_dept_annual",
                "stream": "Sentinel-2 NDVI",
                "beta": float(res.params[1]),
                "se": float(res.bse[1]),
                "t": float(res.tvalues[1]),
                "pvalue": float(res.pvalues[1]),
                "n": int(len(reg)),
                "r2": float(res.rsquared),
                "sample_start": int(reg["year"].min()),
                "sample_end": int(reg["year"].max()),
            })
        else:
            _log_pending("E1-NDVI", f"only {len(reg)} clean panel obs")

    out = ESTIMATES / "elasticities.parquet"
    pd.DataFrame(rows).to_parquet(out, index=False)
    print(f"[E1] wrote {out.relative_to(V3)} ({len(rows)} specifications)")


# ---------- E2: two-factor DFM ---------------------------------------

def e2_dfm() -> None:
    """Per brief D1+D5+D6+D7+D8+D9+D10+D11+D12 all required. D9, D5, D6,
    D7 are currently missing — therefore E2 cannot produce the declared
    9-variable panel.
    """
    required = {
        "D1 quarterly GDP": PROCESSED / "ine_gdp_quarterly.parquet",
        "D5 IBCH cement": PROCESSED / "ibch_cement_monthly.parquet",
        "D6 CNDC electricity": PROCESSED / "cndc_electricity_monthly.parquet",
        "D7 SIN tax": PROCESSED / "sin_tax_monthly.parquet",
        "D8 VIIRS DNB": PROCESSED / "viirs_dnb_monthly.parquet",
        "D9 VNF Chaco": PROCESSED / "vnf_chaco_monthly.parquet",
        "D10 TROPOMI NO2": PROCESSED / "s5p_no2_monthly.parquet",
        "D11 Sentinel-2 NDVI": PROCESSED / "s2_ndvi_monthly.parquet",
        "D12 Aduana imports": PROCESSED / "aduana_imports_monthly.parquet",
    }
    missing = [k for k, p in required.items() if not p.exists()]
    if missing:
        _log_pending("E2", "two-factor DFM requires all of: "
                           + ", ".join(required.keys())
                           + ". Missing: " + ", ".join(missing)
                           + ". Not running partial DFM (per brief "
                             "rule zero: no auto-filled results from "
                             "incomplete panels).")
        return
    # Full implementation lives in src/05_econometrics/dfm_twofactor.py;
    # invoke once all nine Parquets exist.
    _log_pending("E2", "all nine Parquets present but v3 DFM runner "
                       "not yet wired to consume them.")


# ---------- E3: manipulation tests ----------------------------------

def e3_manipulation() -> None:
    # Test 1: requires D1 + D3 + D4 + D9 per brief. D3/D4/D9 missing.
    # Per brief rule one: do not substitute annual WB-GGFR for monthly VNF.
    _log_pending("E3-Test1", "requires D4 (YPFB field-month) and D9 "
                             "(VNF Chaco monthly). Proxy substitution "
                             "with WB-GGFR explicitly prohibited.")

    # Test 2: requires D3 (IGAE) or quarterly GDP MM-aggregated. IGAE is
    # ~2 months; per brief, flag preliminary if <10 obs post-break.
    _log_pending("E3-Test2", "post-Nov-2025 IGAE sample too short "
                             "(brief: <10 obs => preliminary).")

    # Test 3: annual satellite composite vs INE + external forecasters.
    # Requires E2 output. Skipped.
    _log_pending("E3-Test3", "requires E2 composite (not yet produced).")

    # Write an empty verdict frame so the table has a schema to read.
    cols = ["test", "statistic", "pvalue", "verdict", "identifying_assumption"]
    out = ESTIMATES / "manipulation_tests.parquet"
    pd.DataFrame(columns=cols).to_parquet(out, index=False)
    print(f"[E3] wrote empty {out.relative_to(V3)}")


def main() -> None:
    e1_elasticities()
    e2_dfm()
    e3_manipulation()


if __name__ == "__main__":
    main()
