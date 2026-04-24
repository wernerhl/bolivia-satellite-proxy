"""Render the monthly LaTeX report (Palatino, 2–3 pages).

Sections:
  1. Headline CI and sign
  2. Stream-by-stream commentary
  3. Cross-check table vs INE (if present)
  4. Manipulation-flag status
  5. Known data issues
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from _common import abs_path, load_env, paths  # noqa: E402

load_env()


TEMPLATE = r"""\documentclass[11pt,a4paper]{article}
\usepackage[T1]{fontenc}
\usepackage{mathpazo}
\usepackage[margin=1in]{geometry}
\usepackage{booktabs}
\usepackage{graphicx}
\usepackage{hyperref}
\title{Bolivia Satellite-Proxy Monthly Report --- %(month)s}
\author{}
\date{}
\begin{document}
\maketitle
\thispagestyle{empty}

\section*{Headline}
Satellite-only coincident index: \textbf{%(ci)s}. Direction: \textbf{%(dir)s}.

\section*{Stream-by-stream}
\begin{itemize}
  \item \textbf{VIIRS DNB SOL} (11 city buffers, population-weighted z):
        %(viirs_z)s.
  \item \textbf{VNF Chaco radiant heat} (six fields + other; 1400 K floor):
        %(vnf_z)s.%(vnf_flag)s
  \item \textbf{S5P tropospheric NO${}_2$} (La Paz--El Alto + Santa Cruz):
        %(no2_z)s.%(volcanic_flag)s
\end{itemize}

\begin{figure}[h]\centering\includegraphics[width=0.95\linewidth]{%(fig_ci)s}\end{figure}

\section*{Cross-check against INE}
%(cross_check)s

\section*{Manipulation-flag status}
%(manip_status)s

\section*{World Bank GGFR cross-check}
%(wb_crosscheck)s

\section*{INE IGAE disagreement monitor}
%(igae_disagreement)s

\section*{Known data issues}
\begin{itemize}
  \item VIIRS VNP46A3 has a two-month publication lag; current month is never reported.
  \item S5P coverage in La Paz Jan--Mar and Santa Cruz Nov--Mar is reduced by cloud;
        months with $<15$ valid days are reported NA.
  \item Fuel-subsidy elimination (Dec 2025) is a structural break; pre/post means
        reported separately from 2026 onward.
\end{itemize}

\vspace{2ex}\noindent\small\textit{Opinions do not reflect affiliated institutions;
all errors are the author's. Contact: wernerhl@gmail.com.}
\end{document}
"""


def fmt(x) -> str:
    return f"{x:+.2f}" if pd.notna(x) else "n/a"


def main() -> None:
    p = paths()
    ci = pd.read_csv(abs_path(p["data"]["ci"]))
    vnf = pd.read_csv(abs_path(p["data"]["vnf_anomaly"]))
    no2 = pd.read_csv(abs_path(p["data"]["s5p_anomaly"]))

    ci = ci.dropna(subset=["ci"]).sort_values("date")
    row = ci.iloc[-1] if not ci.empty else None
    month = pd.to_datetime(row["date"]).strftime("%Y-%m") if row is not None else "n/a"
    ci_val = float(row["ci"]) if row is not None else float("nan")

    direction = (
        "expanding" if ci_val > 0.5 else "contracting" if ci_val < -0.5 else "flat"
    )

    vnf_flag_active = bool(vnf["flag_manip"].iloc[-1]) if not vnf.empty and "flag_manip" in vnf.columns else False
    manip_status = (
        "\\textbf{FLAGGED}: residual |z| $>$ 2 for two consecutive months. "
        "Review by La Linterna desk within 48h."
        if vnf_flag_active else
        "No active flag. Residuals within $\\pm 2\\sigma$."
    )

    volcanic_recent = (
        no2.get("volcanic_flag", pd.Series(dtype=bool)).fillna(False).tail(3).any()
        if not no2.empty else False
    )

    wb_path = abs_path(p["data"]["vnf_wb_crosscheck"])
    if wb_path.exists():
        wb = json.loads(wb_path.read_text())
        if wb.get("status") == "ok":
            wb_crosscheck = (
                f"Annual $\\Sigma \\mathrm{{RH}}_\\mathrm{{Chaco}}$ vs World Bank "
                f"GGFR Bolivia flare volume (2012--latest, $n={wb['n_years']}$): "
                f"log-log correlation $={wb['corr_log_log']:.2f}$, "
                f"elasticity $\\hat\\beta={wb['elasticity_beta']:+.2f}$. "
                + ("\\textbf{Methodology-review flag active}: |rel.\\ residual| $>$ 0.25 "
                   "for two consecutive recent years."
                   if wb.get("flag_methodology_review") else
                   "Residuals within $\\pm 25\\%$.")
            )
        else:
            wb_crosscheck = f"WB GGFR cross-check pending ({wb.get('status')})."
    else:
        wb_crosscheck = "WB GGFR cross-check not yet run."

    dis_path = abs_path("data/satellite/igae_disagreement.json")
    if dis_path.exists():
        d = json.loads(dis_path.read_text())
        gap = d.get("gap")
        thr = d.get("deviation_threshold_sigma", 1.5)
        if d.get("alert"):
            igae_disagreement = (
                f"\\textbf{{HALT PUBLICATION}}: satellite CI vs IGAE gap "
                f"$={gap:.2f}\\sigma$ (threshold ${thr:.1f}\\sigma$)"
                + ("; benchmark $\\beta$ sign flip also detected" if d.get("beta_sign_flip") else "")
                + ". Route to La Linterna desk within 48h."
            )
        elif gap is None:
            igae_disagreement = "IGAE series not loaded for current month; monitor inactive."
        else:
            igae_disagreement = (
                f"CI vs IGAE gap $={gap:.2f}\\sigma$ (threshold ${thr:.1f}\\sigma$). "
                "No alert."
            )
    else:
        igae_disagreement = "IGAE disagreement monitor not yet run."

    bench_path = abs_path("data/satellite/benchmark_ine.json")
    if bench_path.exists():
        b = json.loads(bench_path.read_text())
        if b.get("status") == "ok":
            betas = b["betas"]
            cross_check = (
                f"Benchmark regression $\\log(\\mathrm{{IGAE}}) = \\alpha + "
                f"\\beta_1\\,\\mathrm{{viirs}} + \\beta_2\\,\\mathrm{{vnf}} + "
                f"\\beta_3\\,\\mathrm{{no2}} + \\gamma X + \\varepsilon$ (HAC SE, "
                f"$n={b['n']}$, $R^2={b['r2']:.2f}$): "
                f"$\\hat\\beta_1={betas['viirs_z']:+.3f}$, "
                f"$\\hat\\beta_2={betas['vnf_z']:+.3f}$, "
                f"$\\hat\\beta_3={betas['no2_z']:+.3f}$."
            )
        else:
            cross_check = f"Benchmark not run ({b.get('status')})."
    else:
        cross_check = "INE IGAE series not yet loaded; benchmark pending."

    tex = TEMPLATE % {
        "month": month,
        "ci": fmt(ci_val),
        "dir": direction,
        "viirs_z": fmt(float(row["viirs_z"])) if row is not None else "n/a",
        "vnf_z": fmt(float(row["vnf_z"])) if row is not None else "n/a",
        "no2_z": fmt(float(row["no2_z"])) if row is not None else "n/a",
        "vnf_flag": " \\textbf{[FLAG]}" if vnf_flag_active else "",
        "volcanic_flag": " \\textit{[volcanic flag recent]}" if volcanic_recent else "",
        "fig_ci": str(abs_path(p["outputs"]["fig_ci"])),
        "cross_check": cross_check,
        "manip_status": manip_status,
        "wb_crosscheck": wb_crosscheck,
        "igae_disagreement": igae_disagreement,
    }

    out_path = abs_path(p["outputs"]["monthly_tex"])
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(tex)
    print(f"[ok] wrote {out_path}")


if __name__ == "__main__":
    main()
