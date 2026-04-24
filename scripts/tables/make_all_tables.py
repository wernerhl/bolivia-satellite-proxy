"""Generate all paper v2 tables (Track H).

Main:     table1_spatial, table2_elasticities, table3_satellite_vs_external,
          table4_manipulation.
Appendix: tableA1_vintages, tableA2_robustness.

Each table writes a booktabs .tex fragment under tables/ that the
paper includes via \\input{tables/tableN.tex}. All numbers pulled from
the frozen data archive; missing cells render as "---".
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

from _common import abs_path, buffers, flares, load_env, ndvi_zones, paths, rois  # noqa: E402

load_env()
OUT = ROOT / "paper" / "tables"
OUT.mkdir(parents=True, exist_ok=True)


def _safe(path: Path) -> dict:
    if path.exists():
        try:
            return json.loads(path.read_text())
        except Exception:
            return {}
    return {}


def _fmt(x, fmt: str = "{:+.3f}") -> str:
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return "---"
    try:
        return fmt.format(float(x))
    except Exception:
        return "---"


def _stars(p) -> str:
    try:
        p = float(p)
    except Exception:
        return ""
    return "***" if p < 0.01 else "**" if p < 0.05 else "*" if p < 0.10 else ""


# ---------- Table 1: spatial definitions (consolidated) --------------

def table1_spatial() -> None:
    lines: list[str] = [
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
    for c in buffers():
        lines.append(
            rf"& {c['label']} & ${c['lat']:+.3f}$ & ${c['lon']:+.3f}$ "
            rf"& {c['radius_km']} & pop.\ {c['population']:,} \\")
    lines.append(r"\midrule")
    lines.append(r"\multicolumn{6}{l}{\textit{Chaco gas fields (VIIRS Nightfire)}} \\")
    for f in flares()["fields"]:
        lines.append(
            rf"& {f['name'].replace('_',' ').title()} & ${f['lat']:+.3f}$ "
            rf"& ${f['lon']:+.3f}$ & 2 (radius) & {f['operator']}, {f['notes']} \\")
    lines.append(r"\midrule")
    lines.append(r"\multicolumn{6}{l}{\textit{Metropolitan NO$_2$ ROIs (Sentinel-5P TROPOMI)}} \\")
    for r in rois():
        dlat = abs(r["nw_lat"] - r["se_lat"]) * 111
        dlon = abs(r["se_lon"] - r["nw_lon"]) * 111
        lines.append(
            rf"& {r['label']} & --- & --- & "
            rf"{dlat:.0f}$\times${dlon:.0f} & rectangle \\")
    lines.append(r"\midrule")
    lines.append(r"\multicolumn{6}{l}{\textit{Cropland zones (Sentinel-2 NDVI)}} \\")
    for z in ndvi_zones():
        lines.append(
            rf"& {z['label']} & ${z['lat']:+.3f}$ & ${z['lon']:+.3f}$ "
            rf"& {z['radius_km']} & GVA weight {z['gva_weight']:.2f}; "
            rf"{z['dominant_crops']} \\")
    lines.extend([r"\bottomrule", r"\end{tabular}", r"\end{table}"])
    (OUT / "table1_spatial.tex").write_text("\n".join(lines))
    print("[ok] table1_spatial.tex")


# ---------- Table 2: single-series elasticities ----------------------

def table2_elasticities() -> None:
    v = _safe(abs_path("data/satellite/elasticity_viirs.json"))
    f = _safe(abs_path("data/satellite/elasticity_vnf.json"))
    n = _safe(abs_path("data/satellite/elasticity_no2.json"))

    def row(name: str, spec: str, d: dict, ref: str) -> str:
        if d.get("status") != "ok":
            return rf"{name} & {spec} & --- & --- & --- & --- & {ref} \\"
        beta = _fmt(d["beta"], "{:+.3f}")
        se = _fmt(d["se"], "{:.3f}")
        stars = _stars(d.get("p"))
        return (rf"{name} & {spec} & ${beta}^{{{stars}}}$ ({se}) & "
                rf"{d.get('n','---')} & {_fmt(d.get('r2'),'{:.2f}')} "
                rf"& --- & {ref} \\")

    lines = [
        r"\begin{table}[H]",
        r"\centering\small",
        r"\caption{Single-series elasticities. Two-way-clustered (CGM 2011) "
        r"standard errors in parentheses. $^{*}$, $^{**}$, $^{***}$: "
        r"$p<0.10, 0.05, 0.01$.}",
        r"\label{tab:elasticities}",
        r"\begin{tabular}{lllrrrl}",
        r"\toprule",
        r"Stream & Spec. & $\hat\beta$ (SE) & $n$ & $R^2$ & Range & Benchmark \\",
        r"\midrule",
        row("VIIRS DNB $\\to$ GDP", "Eq.~(1)", v, r"\citet{hsw2012} $\approx 0.30$"),
        row("VNF $\\to$ gas prod.",   "Eq.~(2)", f, r"\citet{do_etal2018} $\approx 1.0$"),
        row(r"NO$_2$ $\to$ fuel sales", "Eq.~(3)", n, r"\citet{bauwens2020} $0.2$--$0.5$"),
        r"NDVI $\to$ ag.\ GVA & Eq.~(4) & --- & --- & --- & --- & Johnson (2014) \\",
        r"\bottomrule", r"\end{tabular}", r"\end{table}",
    ]
    (OUT / "table2_elasticities.tex").write_text("\n".join(lines))
    print("[ok] table2_elasticities.tex")


# ---------- Table 3: composite vs INE vs forecasters -----------------

def table3_satellite_vs_external() -> None:
    ine = abs_path("data/official/ine_gdp_quarterly.csv")
    ext = abs_path("data/official/external_forecasters.csv")
    ci = abs_path(paths()["data"]["ci"])

    # INE annual growth from quarterly: mean of y/y growth across 4 quarters
    ine_annual = {}
    if ine.exists():
        df = pd.read_csv(ine, parse_dates=["date"]).sort_values("date")
        import numpy as np
        df["log_gdp"] = np.log(df["gdp_real"])
        df["yoy"] = df["log_gdp"] - df["log_gdp"].shift(4)
        df["year"] = df["date"].dt.year
        ine_annual = df.dropna(subset=["yoy"]).groupby("year")["yoy"].mean().to_dict()

    # Satellite composite annual mean z-score (not a growth rate; we show
    # direction and magnitude)
    ci_annual = {}
    if ci.exists():
        cf = pd.read_csv(ci, parse_dates=["date"]).dropna(subset=["ci"])
        cf["year"] = cf["date"].dt.year
        ci_annual = cf.groupby("year")["ci"].mean().to_dict()

    ext_df = pd.read_csv(ext) if ext.exists() else pd.DataFrame()
    years = [2023, 2024, 2025, 2026]

    lines = [
        r"\begin{table}[H]",
        r"\centering\small",
        r"\caption{Annual comparison: INE, satellite composite, and external "
        r"forecasters. INE growth in percent; satellite composite in mean "
        r"annual z-score; external forecaster growth in percent.}",
        r"\label{tab:sat_vs_ine}",
        r"\begin{tabular}{lrrrrrr}",
        r"\toprule",
        r"Year & INE (\%) & Sat.\ mean $z$ & IMF (\%) & WB (\%) & Oxford (\%) & S\&P (\%) \\",
        r"\midrule",
    ]
    for y in years:
        ine_v = ine_annual.get(y)
        ci_v = ci_annual.get(y)
        ext_row = ext_df[ext_df["year"] == y].squeeze() if (not ext_df.empty and (ext_df["year"] == y).any()) else None
        ine_cell = _fmt(ine_v * 100 if ine_v is not None else None, "{:+.2f}")
        ci_cell = _fmt(ci_v, "{:+.2f}")
        def _ecell(col):
            if ext_row is None: return "---"
            v = ext_row.get(col)
            return _fmt(v, "{:+.2f}")
        lines.append(rf"{y} & {ine_cell} & {ci_cell} & "
                     rf"{_ecell('imf')} & {_ecell('wb')} & "
                     rf"{_ecell('oxford')} & {_ecell('snp')} \\")
    lines.extend([r"\bottomrule", r"\end{tabular}", r"\end{table}"])
    (OUT / "table3_satellite_vs_external.tex").write_text("\n".join(lines))
    print("[ok] table3_satellite_vs_external.tex")


# ---------- Table 4: manipulation-detection suite --------------------

def table4_manipulation() -> None:
    d = _safe(abs_path("data/satellite/manipulation_tests.json"))
    t1 = d.get("test1_sectoral_triangulation", {})
    t2 = d.get("test2_leadership_discontinuity", {})
    t3 = d.get("test3_external_forecaster_residual", {})

    def _cell_statistic(t: dict, keys: list[str]) -> str:
        for k in keys:
            if k in t and t[k] is not None:
                return _fmt(t[k], "{:.3f}")
        return "---"

    lines = [
        r"\begin{table}[H]",
        r"\centering\small",
        r"\caption{Manipulation-detection suite from \S~4.3.}",
        r"\label{tab:manipulation}",
        r"\begin{tabular}{p{3.2cm}p{5.8cm}p{2cm}p{2cm}p{2cm}}",
        r"\toprule",
        r"Test & Identifying assumption (one line) & Statistic & Verdict \\",
        r"\midrule",
        rf"1.\ Sectoral triangulation (VNF--YPFB--INE) & "
        rf"Flaring is a physical observation no Bolivian institution controls. "
        rf"& corr pre: {_cell_statistic(t1,['vnf_ypfb_corr'])} "
        rf"/ {_cell_statistic(t1,['ypfb_ine_corr'])} "
        rf"& {t1.get('verdict','---').replace('_',' ') if t1.get('status')=='ok' else t1.get('status','---')} \\",
        r"\midrule",
        rf"2.\ Nov-2025 INE leadership discontinuity & "
        rf"Fundamentals continuous across the leadership change. "
        rf"& $\hat\gamma = {_cell_statistic(t2,['gamma_interaction'])}$, "
        rf"$p = {_cell_statistic(t2,['gamma_pvalue'])}$ "
        rf"& {t2.get('verdict','---').replace('_',' ') if t2.get('status')=='ok' else t2.get('status','---')} \\",
        r"\midrule",
        rf"3.\ External-forecaster residual & "
        rf"Consensus and satellite draw on the same macro-data universe but "
        rf"produce independent numbers. "
        rf"& INE res.\ mean: {_cell_statistic(t3,['ine_residual_crisis_mean'])} "
        rf"vs ext.\ {_cell_statistic(t3,['ext_residual_crisis_mean'])} "
        rf"& {t3.get('verdict','---').replace('_',' ') if t3.get('status')=='ok' else t3.get('status','---')} \\",
        r"\bottomrule", r"\end{tabular}", r"\end{table}",
    ]
    (OUT / "table4_manipulation.tex").write_text("\n".join(lines))
    print("[ok] table4_manipulation.tex")


# ---------- Appendix Table A1: vintages ------------------------------

def tableA1_vintages() -> None:
    rows = [
        ("INE quarterly GDP",          "INE Nextcloud xlsx",     "Q", "60--90",   "1990Q1--2024Q4", "stable post-vintage"),
        ("INE IGAE",                    "INE portal",             "M", "45",       "starts Mar 2026", "preliminary"),
        ("INE departmental GDP",        "INE annual xlsx",        "A", "365--700", "---",            "stable"),
        ("YPFB gas prod.\\ (total)",   "YPFB JPG chart",         "A", "90",       "2006--2025",     "preliminary"),
        ("YPFB field-month prod.",     "Boletín Estadístico",    "M", "30--60",   "---",            "stable"),
        ("IBCH cement dispatches",     "IBCH publications",      "M", "20--40",   "---",            "stable"),
        ("CNDC electricity",           "CNDC dload.php xlsx",    "M", "5--10",    "---",            "stable"),
        ("SIN tax collections",        "SIN portal",             "M", "30",       "---",            "stable"),
        ("Aduana imports",             "INE Nextcloud xlsx",     "M", "30--45",   "2024-01--2026-02", "stable"),
        ("BCB monetary",                "BCB SPM",                "M", "15--30",   "---",            "revised"),
        ("VIIRS DNB (VNP46A2)",        "GEE",                    "D", "3--5",     "2012-01--2026-04", "BRDF gap-filled"),
        ("VIIRS Nightfire",            "EOG",                    "D", "1--2",     "---",            "stable"),
        (r"TROPOMI NO$_2$",            "GEE OFFL",               "D", "5--10",    "2018-07--2026-04", "QA pre-filtered"),
        ("Sentinel-2 NDVI",            "GEE HARMONIZED",         "D", "3--5",     "2017-03--(pend.)", "SCL-masked"),
        ("WB GGFR flaring",            "WB xlsx",                "A", "180",      "2012--2024",     "stable"),
        ("Binance P2P premium",        "Binance P2P API",        "D", "0",        "2026-04-24+",    "real-time"),
    ]
    lines = [
        r"\begin{table}[H]",
        r"\centering\scriptsize",
        r"\caption{Data vintages, release lags, and revision behavior as of "
        r"the submission vintage. Lag = nominal days between period end and "
        r"source publication.}",
        r"\label{tab:vintages}",
        r"\begin{tabular}{p{3.0cm}p{3.0cm}cclp{2.2cm}}",
        r"\toprule",
        r"Series & Source & Freq. & Lag (d) & Coverage in paper & Revision \\",
        r"\midrule",
    ]
    for s, src, f, lag, cov, rev in rows:
        lines.append(rf"{s} & {src} & {f} & {lag} & {cov} & {rev} \\")
    lines.extend([r"\bottomrule", r"\end{tabular}", r"\end{table}"])
    (OUT / "tableA1_vintages.tex").write_text("\n".join(lines))
    print("[ok] tableA1_vintages.tex")


# ---------- Appendix Table A2: robustness grid (stub) ----------------

def tableA2_robustness() -> None:
    lines = [
        r"\begin{table}[H]",
        r"\centering\scriptsize",
        r"\caption{Robustness grid: trough date and depth under alternative "
        r"specifications. Cells marked --- indicate the specification has not "
        r"yet been exercised in the current vintage; see notes below.}",
        r"\label{tab:robustness}",
        r"\begin{tabular}{lllrr}",
        r"\toprule",
        r"Specification & Trough date & Trough $\sigma$ & 2025 (\%) & 2026 (\%) \\",
        r"\midrule",
        r"Baseline (two-factor DFM, 2012-baseline) & 2024-01 & $-2.62$ & --- & --- \\",
        r"VIIRS only & --- & --- & --- & --- \\",
        r"VNF only & --- & --- & --- & --- \\",
        r"NO$_2$ only & --- & --- & --- & --- \\",
        r"NDVI only & --- & --- & --- & --- \\",
        r"VNF radius 1 km / 2 km / 3 km & --- & --- & --- & --- \\",
        r"VNF threshold 1200 K / 1400 K / 1600 K & --- & --- & --- & --- \\",
        r"TROPOMI qa\_value 0.50 / 0.75 / 0.90 & --- & --- & --- & --- \\",
        r"One-factor vs two-factor vs weighted composite & --- & --- & --- & --- \\",
        r"Baseline 2013--2019 vs 2015--2019 vs 2017--2019 & --- & --- & --- & --- \\",
        r"Population vs GDP weights & --- & --- & --- & --- \\",
        r"\bottomrule", r"\end{tabular}", r"\end{table}",
    ]
    (OUT / "tableA2_robustness.tex").write_text("\n".join(lines))
    print("[ok] tableA2_robustness.tex")


def main() -> None:
    table1_spatial()
    table2_elasticities()
    table3_satellite_vs_external()
    table4_manipulation()
    tableA1_vintages()
    tableA2_robustness()


if __name__ == "__main__":
    main()
