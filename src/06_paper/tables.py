"""Generate LaTeX tables consumed by paper/fires_lights_smog.tex.

Each table is written as a standalone .tex file under paper/tables/
and is meant to be \\input'ed from the main document. The main .tex
currently holds the city/flare/ROI descriptor tables inline; this
module produces the results-section tables that are presently
placeholder \\tbdline{...} commands.

  paper/tables/elasticities.tex       — eqs (6)(7)(8) β̂, SE, n, comparison to lit
  paper/tables/manipulation.tex       — three manipulation tests summary
  paper/tables/ine_vs_satellite.tex   — quarterly INE growth vs satellite factor
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from _common import abs_path, load_env  # noqa: E402

load_env()


def _safe(path: Path) -> dict:
    if path.exists():
        try:
            return json.loads(path.read_text())
        except Exception:
            return {}
    return {}


def _f(x, fmt: str = "{:+.3f}") -> str:
    if x is None:
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
    return "***" if p < 0.01 else "**" if p < 0.05 else "*" if p < 0.1 else ""


def elasticities_tex(out: Path) -> None:
    v = _safe(abs_path("data/satellite/elasticity_viirs.json"))
    f = _safe(abs_path("data/satellite/elasticity_vnf.json"))
    n = _safe(abs_path("data/satellite/elasticity_no2.json"))

    def row(name: str, d: dict, lit: str) -> str:
        if d.get("status") != "ok":
            return rf"{name} & --- & --- & --- & {lit} \\"
        beta = _f(d.get("beta"), "{:+.3f}")
        se = _f(d.get("se"), "{:.3f}")
        stars = _stars(d.get("p"))
        return rf"{name} & ${beta}^{{{stars}}}$ & ({se}) & {d.get('n','')} & {lit} \\"

    tex = rf"""\begin{{table}}[H]
\centering
\caption{{Single-series elasticities. Two-way-clustered SEs in parentheses.
$^{{*}}$, $^{{**}}$, $^{{***}}$: $p<0.10,0.05,0.01$.}}
\label{{tab:elasticities}}
\small
\begin{{tabular}}{{lrrrl}}
\toprule
Specification & $\hat\beta$ & (SE) & $n$ & Benchmark from literature \\
\midrule
{row('Eq.~(6) VIIRS $\\to$ GDP (annual, department)', v, '\\citet{hsw2012}: 0.30')}
{row('Eq.~(7) VNF $\\to$ gas production (monthly, field)', f, '\\citet{do_etal2018}: $\\approx 1.0$')}
{row('Eq.~(8) NO$_2$ $\\to$ fuel sales (monthly, metro)', n, '\\citet{bauwens2020}: 0.2--0.5')}
\bottomrule
\end{{tabular}}
\end{{table}}
"""
    out.write_text(tex)
    print(f"[ok] {out.relative_to(abs_path('.'))}")


def manipulation_tex(out: Path) -> None:
    d = _safe(abs_path("data/satellite/manipulation_tests.json"))
    t1 = d.get("test1_pre_post", {})
    t2 = d.get("test2_residual", {})
    t3 = d.get("test3_sectoral", {})

    ht = t1.get("high_trust_2006_2014", {})
    lt = t1.get("low_trust_2020_2024", {})

    tex = rf"""\begin{{table}}[H]
\centering
\caption{{Manipulation-detection suite (\S~4.3). All three tests jointly.}}
\label{{tab:manipulation}}
\small
\begin{{tabular}}{{llll}}
\toprule
Test & Quantity & Estimate & Verdict \\
\midrule
(1) Pre/post trust & $\hat\beta^{{\text{{high-trust}}}}$ (2006--2014) &
    {_f(ht.get('beta'))} ({_f(ht.get('se'),'{:.3f}')}), $n={ht.get('n','---')}$ & \multirow{{2}}{{*}}{{{t1.get('verdict','---').replace('_','-')}}} \\
                   & $\hat\beta^{{\text{{low-trust}}}}$ (2020--2024) &
    {_f(lt.get('beta'))} ({_f(lt.get('se'),'{:.3f}')}), $n={lt.get('n','---')}$ & \\
\midrule
(2) Residual (factor on IGAE-$g$) & post-2023Q4 residual mean &
    {_f(t2.get('post_mean'))}, $t={_f(t2.get('post_mean_t'),'{:.2f}')}$ & {t2.get('post_sign','---').split('(')[0].strip()} \\
\midrule
(3) Sectoral & corr$(\log \Sigma\!RH, \log GasProd)$ &
    {_f(t3.get('vnf_ypfb_corr'),'{:.2f}')} & \multirow{{2}}{{*}}{{{t3.get('verdict','---').replace('_','-')}}} \\
             & corr$(\log GasProd, \log VA^{{hydro}})$ &
    {_f(t3.get('ypfb_ine_corr'),'{:.2f}')} & \\
\bottomrule
\end{{tabular}}
\end{{table}}
"""
    out.write_text(tex)
    print(f"[ok] {out.relative_to(abs_path('.'))}")


def ine_vs_satellite_tex(out: Path) -> None:
    p = _safe(abs_path("data/satellite/benchmark_ine.json"))
    wb = _safe(abs_path("data/satellite/vnf_wb_crosscheck.json"))
    disag = _safe(abs_path("data/satellite/igae_disagreement.json"))

    betas = p.get("betas", {}) if p.get("status") == "ok" else {}
    tex = rf"""\begin{{table}}[H]
\centering
\caption{{INE benchmark regression and external cross-checks.}}
\label{{tab:ine_vs_sat}}
\small
\begin{{tabular}}{{lr}}
\toprule
Quantity & Value \\
\midrule
$\hat\beta_1$ (VIIRS on $\log$IGAE, HAC)  & {_f(betas.get('viirs_z'))} \\
$\hat\beta_2$ (VNF on $\log$IGAE, HAC)    & {_f(betas.get('vnf_z'))} \\
$\hat\beta_3$ (NO$_2$ on $\log$IGAE, HAC) & {_f(betas.get('no2_z'))} \\
$R^2$ (INE benchmark)                     & {_f(p.get('r2'),'{:.2f}')} \\
$n$ (INE benchmark)                       & {p.get('n','---')} \\
\midrule
WB GGFR log-log corr (annual Chaco)       & {_f(wb.get('corr_log_log'),'{:.2f}')} \\
WB GGFR elasticity $\hat\beta$            & {_f(wb.get('elasticity_beta'))} \\
WB GGFR $n$ (years)                       & {wb.get('n_years','---')} \\
\midrule
Latest CI vs IGAE-$z$ gap ($\sigma$)      & {_f(disag.get('gap'),'{:.2f}')} \\
Disagreement threshold ($\sigma$)         & {_f(disag.get('deviation_threshold_sigma'),'{:.1f}')} \\
\bottomrule
\end{{tabular}}
\end{{table}}
"""
    out.write_text(tex)
    print(f"[ok] {out.relative_to(abs_path('.'))}")


def main() -> None:
    out_dir = abs_path("paper/tables")
    out_dir.mkdir(parents=True, exist_ok=True)
    elasticities_tex(out_dir / "elasticities.tex")
    manipulation_tex(out_dir / "manipulation.tex")
    ine_vs_satellite_tex(out_dir / "ine_vs_satellite.tex")


if __name__ == "__main__":
    main()
