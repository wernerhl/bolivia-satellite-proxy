"""V3 table builder — produces the 4 main + 2 appendix booktabs LaTeX
fragments declared in the brief.

Rule: any cell whose underlying estimate has not been produced reads
exactly `---`. No "inputs_missing", no "insufficient_n", no fake
numbers or placeholder CIs.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import yaml

HERE = Path(__file__).resolve().parent
V3 = HERE.parent

PROCESSED = V3 / "data" / "processed"
ESTIMATES = V3 / "data" / "estimates"
OUT = V3 / "tables"
OUT.mkdir(parents=True, exist_ok=True)
SPATIAL = yaml.safe_load((V3 / "config" / "spatial.yaml").read_text())


def _fmt(x, fmt: str = "{:+.3f}") -> str:
    if x is None:
        return "---"
    try:
        if pd.isna(x):
            return "---"
        return fmt.format(float(x))
    except Exception:
        return "---"


def _stars(p) -> str:
    try:
        p = float(p)
    except Exception:
        return ""
    return "***" if p < 0.01 else "**" if p < 0.05 else "*" if p < 0.10 else ""


# ---------- Table 1: spatial definitions ----------------------------

def table1_spatial() -> None:
    lines = [
        r"\begin{table}[H]",
        r"\centering\small",
        r"\caption{Spatial definitions for the four satellite streams. "
        r"Coordinates are centroid latitude and longitude in WGS84. Extent "
        r"is a circular buffer radius in km or, for TROPOMI, a rectangle.}",
        r"\label{tab:spatial}",
        r"\begin{tabular}{llrrrl}",
        r"\toprule",
        r"Section & Name & Lat & Lon & Extent (km) & Notes \\",
        r"\midrule",
        r"\multicolumn{6}{l}{\textit{Urban buffers (VIIRS DNB)}} \\",
    ]
    for c in SPATIAL["urban_buffers"]:
        lines.append(
            rf"& {c['label']} & ${c['lat']:+.3f}$ & ${c['lon']:+.3f}$ "
            rf"& {c['radius_km']} & pop.\ {c['population']:,} \\")
    lines.append(r"\midrule")
    lines.append(r"\multicolumn{6}{l}{\textit{Chaco gas fields (VIIRS Nightfire)}} \\")
    for f in SPATIAL["chaco"]["fields"]:
        lines.append(
            rf"& {f['label']} & ${f['lat']:+.3f}$ "
            rf"& ${f['lon']:+.3f}$ & 2 (radius) & {f['operator']} \\")
    lines.append(r"\midrule")
    lines.append(r"\multicolumn{6}{l}{\textit{Metropolitan NO$_2$ ROIs (Sentinel-5P TROPOMI)}} \\")
    for r in SPATIAL["tropomi_rois"]:
        dlat = abs(r["nw_lat"] - r["se_lat"]) * 111
        dlon = abs(r["se_lon"] - r["nw_lon"]) * 111
        lines.append(
            rf"& {r['label']} & --- & --- & "
            rf"{dlat:.0f}$\times${dlon:.0f} & rectangle \\")
    lines.append(r"\midrule")
    lines.append(r"\multicolumn{6}{l}{\textit{Cropland zones (Sentinel-2 NDVI)}} \\")
    for z in SPATIAL["cropland_zones"]:
        lines.append(
            rf"& {z['label']} & ${z['lat']:+.3f}$ & ${z['lon']:+.3f}$ "
            rf"& {z['radius_km']} & GVA weight {z['gva_weight']:.2f}, "
            rf"{z['dominant_crops']} \\")
    lines.extend([r"\bottomrule", r"\end{tabular}", r"\end{table}"])
    (OUT / "table1_spatial.tex").write_text("\n".join(lines))
    print("[T1] table1_spatial.tex")


# ---------- Table 2: elasticities -----------------------------------

def table2_elasticities() -> None:
    ep = ESTIMATES / "elasticities.parquet"
    est = pd.read_parquet(ep) if ep.exists() else pd.DataFrame()

    def _row(stream: str, spec: str, ref: str) -> str:
        r = next((r for _, r in est.iterrows()
                  if r["stream"].split()[0] == stream.split()[0]), None)
        if r is None or pd.isna(r.get("beta")):
            return rf"{stream} & {spec} & --- & --- & --- & --- & {ref} \\"
        beta = _fmt(r["beta"], "{:+.3f}")
        se = _fmt(r.get("se"), "{:.3f}")
        stars = _stars(r.get("pvalue"))
        return (rf"{stream} & {spec} & ${beta}^{{{stars}}}$ ({se}) & "
                rf"{int(r['n'])} & {_fmt(r.get('r2'),'{:.2f}')} "
                rf"& --- & {ref} \\")

    lines = [
        r"\begin{table}[H]",
        r"\centering\small",
        r"\caption{Single-series elasticities. Two-way-clustered "
        r"standard errors in parentheses. Stars: $^{*}$ $p<0.10$, "
        r"$^{**}$ $p<0.05$, $^{***}$ $p<0.01$.}",
        r"\label{tab:elasticities}",
        r"\begin{tabular}{llllrrl}",
        r"\toprule",
        r"Stream & Spec. & $\hat\beta$ (SE) & $n$ & $R^2$ & Range & Benchmark \\",
        r"\midrule",
        _row("VIIRS DNB", "Eq.~(1)", r"\citet{hsw2012} $\approx 0.30$"),
        _row("VNF", "Eq.~(2)", r"\citet{do_etal2018} $\approx 1.0$"),
        _row(r"NO$_2$", "Eq.~(3)", r"\citet{bauwens2020} $0.2$--$0.5$"),
        _row("Sentinel-2", "Eq.~(4)", r"Johnson (2014)"),
        r"\bottomrule", r"\end{tabular}", r"\end{table}",
    ]
    (OUT / "table2_elasticities.tex").write_text("\n".join(lines))
    print("[T2] table2_elasticities.tex")


# ---------- Table 3: satellite composite vs INE vs forecasters -----

def table3_satellite_vs_external() -> None:
    # Composite requires E2 which is blocked. All composite cells = ---.
    ine_path = PROCESSED / "ine_gdp_quarterly.parquet"
    ext_path = PROCESSED / "external_forecasts_annual.parquet"

    ine_annual: dict[int, float] = {}
    if ine_path.exists():
        df = pd.read_parquet(ine_path)
        total = df[df["sector_code"].str.contains(
            "PIB.*PRECIOS.*MERCADO|PRODUCTO.*INTERNO.*BRUTO", na=False, regex=True)]
        if total.empty:
            # Fallback to the TOTAL sector code if matchable
            total = df[df["sector_code"].str.contains("PIB", na=False)]
        total = total.drop_duplicates(subset=["date"])
        total["year"] = total["date"].dt.year
        total["log"] = pd.to_numeric(total["real_value_1990base"],
                                       errors="coerce").apply(
            lambda v: float("nan") if v is None else v)
        import numpy as np
        grouped = total.groupby("year")["real_value_1990base"].sum()
        # y/y annual growth from sum of quarters
        for y in grouped.index:
            prev = grouped.get(y - 1)
            if prev and grouped[y]:
                ine_annual[y] = float(np.log(grouped[y]) - np.log(prev))

    ext_df = pd.read_parquet(ext_path) if ext_path.exists() else pd.DataFrame()
    years = [2023, 2024, 2025, 2026]
    lines = [
        r"\begin{table}[H]",
        r"\centering\small",
        r"\caption{Annual comparison: INE reported growth, satellite "
        r"composite, and external forecasters. INE and forecasters in "
        r"percent; satellite composite annualized z-score from the "
        r"two-factor DFM when estimated.}",
        r"\label{tab:sat_vs_ine}",
        r"\begin{tabular}{lrrrrrr}",
        r"\toprule",
        r"Year & INE (\%) & Sat.\ comp. & IMF (\%) & WB (\%) & Oxford (\%) & S\&P (\%) \\",
        r"\midrule",
    ]
    for y in years:
        ine_cell = _fmt(ine_annual.get(y) * 100 if y in ine_annual else None,
                          "{:+.2f}")
        # Composite not estimated
        comp_cell = "---"
        def _ef(col):
            if ext_df.empty:
                return "---"
            r = ext_df[(ext_df["year"] == y) & (ext_df["forecaster"] == col)]
            if r.empty:
                return "---"
            v = r.iloc[0]["forecast_real_gdp_growth_pct"]
            return _fmt(v, "{:+.2f}")
        lines.append(rf"{y} & {ine_cell} & {comp_cell} & "
                     rf"{_ef('imf')} & {_ef('wb')} & "
                     rf"{_ef('oxford')} & {_ef('snp')} \\")
    lines.extend([r"\bottomrule", r"\end{tabular}", r"\end{table}"])
    (OUT / "table3_satellite_vs_external.tex").write_text("\n".join(lines))
    print("[T3] table3_satellite_vs_external.tex")


# ---------- Table 4: manipulation suite -----------------------------

def table4_manipulation() -> None:
    # E3 produces an empty DataFrame this vintage. All verdict cells = ---.
    lines = [
        r"\begin{table}[H]",
        r"\centering\small",
        r"\caption{Manipulation-detection suite from \S~4.3. All tests "
        r"require data not yet in the frozen archive; verdicts pending.}",
        r"\label{tab:manipulation}",
        r"\begin{tabular}{p{3.2cm}p{6cm}p{2.2cm}p{2.2cm}}",
        r"\toprule",
        r"Test & Identifying assumption & Statistic & Verdict \\",
        r"\midrule",
        r"1.\ Sectoral triangulation (VNF--YPFB--INE) & "
        r"Flaring is a physical observation no Bolivian institution "
        r"controls. & --- & --- \\",
        r"\midrule",
        r"2.\ Nov-2025 INE leadership discontinuity & "
        r"Fundamentals continuous across the leadership change; any "
        r"satellite-to-GDP elasticity shift is statistical production. "
        r"& --- & --- \\",
        r"\midrule",
        r"3.\ External-forecaster residual & "
        r"Consensus and satellite draw on the same macro-data universe "
        r"but produce independent numbers. & --- & --- \\",
        r"\bottomrule", r"\end{tabular}", r"\end{table}",
    ]
    (OUT / "table4_manipulation.tex").write_text("\n".join(lines))
    print("[T4] table4_manipulation.tex")


# ---------- Appendix Table A1: vintages -----------------------------

def tableA1_vintages() -> None:
    # Compute actual coverage in this vintage
    def _coverage(fn: str, date_col: str = "date") -> str:
        p = PROCESSED / fn
        if not p.exists():
            return "---"
        df = pd.read_parquet(p)
        if date_col in df.columns:
            dmin = pd.to_datetime(df[date_col].min()).strftime("%Y-%m")
            dmax = pd.to_datetime(df[date_col].max()).strftime("%Y-%m")
            return f"{dmin}--{dmax}"
        elif "year" in df.columns:
            return f"{int(df['year'].min())}--{int(df['year'].max())}"
        return "present"

    rows = [
        ("INE quarterly GDP",    "INE Nextcloud",  "Q",  "60--90",  _coverage("ine_gdp_quarterly.parquet"), "1990 base"),
        ("INE IGAE",             "INE portal",     "M",  "45",      _coverage("ine_igae_monthly.parquet"),  "preliminary"),
        ("INE departmental GDP", "INE annual",     "A",  "365--700", _coverage("ine_dep_gdp_annual.parquet"), "1990 base"),
        ("YPFB field-month",     "BEH PDF annex",  "M",  "30--60",  _coverage("ypfb_field_monthly.parquet"), "---"),
        ("IBCH cement",          "IBCH / Milenio", "M",  "20--40",  _coverage("ibch_cement_monthly.parquet"), "---"),
        ("CNDC electricity",     "CNDC xlsx",      "D",  "5--10",   _coverage("cndc_electricity_daily.parquet"), "---"),
        ("SIN tax",              "SIN portal",     "M",  "30",      _coverage("sin_tax_monthly.parquet"), "---"),
        ("Aduana imports",       "INE Nextcloud",  "M",  "30--45",  _coverage("aduana_imports_monthly.parquet"), "CIF / FOB / kg"),
        ("BCB monetary",         "BCB SPM PDF",    "M",  "15--30",  _coverage("bcb_monetary_monthly.parquet"), "---"),
        ("VIIRS DNB",            "GEE VNP46A2",    "D",  "3--5",    _coverage("viirs_dnb_monthly.parquet"), "BRDF gap-filled"),
        ("VIIRS Nightfire",      "EOG VNF",        "D",  "1--2",    _coverage("vnf_chaco_monthly.parquet"), "Chaco bbox"),
        (r"TROPOMI NO$_2$",      "GEE OFFL",       "D",  "5--10",   _coverage("s5p_no2_monthly.parquet"), "QA $\\geq$ 0.75"),
        ("Sentinel-2 NDVI",      "GEE HARMONIZED", "D",  "3--5",    _coverage("s2_ndvi_monthly.parquet"), "+ L8 splice"),
        ("External forecasters", "IMF/WB/Oxford/SP", "A", "0", _coverage("external_forecasts_annual.parquet", "year"), "2026-04-24"),
    ]
    lines = [
        r"\begin{table}[H]",
        r"\centering\scriptsize",
        r"\caption{Data vintages, release lags, and coverage in this "
        r"vintage. Lag is nominal days between period end and source "
        r"publication. `---' indicates the series has not been fetched "
        r"for this vintage.}",
        r"\label{tab:vintages}",
        r"\begin{tabular}{p{2.8cm}p{2.6cm}cclp{2.2cm}}",
        r"\toprule",
        r"Series & Source & Freq.\ & Lag (d) & Coverage & Notes \\",
        r"\midrule",
    ]
    for s, src, freq, lag, cov, notes in rows:
        lines.append(rf"{s} & {src} & {freq} & {lag} & {cov} & {notes} \\")
    lines.extend([r"\bottomrule", r"\end{tabular}", r"\end{table}"])
    (OUT / "tableA1_vintages.tex").write_text("\n".join(lines))
    print("[TA1] tableA1_vintages.tex")


# ---------- Appendix Table A2: robustness grid ----------------------

def tableA2_robustness() -> None:
    # E2 baseline not run. Per brief: do not populate baseline while
    # leaving alternatives empty. All rows --- until baseline runs.
    lines = [
        r"\begin{table}[H]",
        r"\centering\scriptsize",
        r"\caption{Robustness grid. Baseline DFM and alternative "
        r"specifications both pending; populating baseline while leaving "
        r"alternatives empty would create the false appearance that a "
        r"full robustness exercise has been conducted (brief rule zero).}",
        r"\label{tab:robustness}",
        r"\begin{tabular}{lllrr}",
        r"\toprule",
        r"Specification & Trough date & Trough $\sigma$ & 2025 (\%) & 2026 (\%) \\",
        r"\midrule",
        r"Baseline (two-factor DFM) & --- & --- & --- & --- \\",
        r"VIIRS only & --- & --- & --- & --- \\",
        r"VNF only & --- & --- & --- & --- \\",
        r"NO$_2$ only & --- & --- & --- & --- \\",
        r"NDVI only & --- & --- & --- & --- \\",
        r"Single-factor DFM vs two-factor vs LMST composite & --- & --- & --- & --- \\",
        r"Alternative baseline windows & --- & --- & --- & --- \\",
        r"Population vs GDP weights & --- & --- & --- & --- \\",
        r"\bottomrule", r"\end{tabular}", r"\end{table}",
    ]
    (OUT / "tableA2_robustness.tex").write_text("\n".join(lines))
    print("[TA2] tableA2_robustness.tex")


def main() -> None:
    table1_spatial()
    table2_elasticities()
    table3_satellite_vs_external()
    table4_manipulation()
    tableA1_vintages()
    tableA2_robustness()


if __name__ == "__main__":
    main()
