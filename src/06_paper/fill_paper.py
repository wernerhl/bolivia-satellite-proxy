"""Patch paper/fires_lights_smog.tex with pipeline outputs.

We do not regenerate the whole paper — the author writes the prose.
We only:
  * replace the Results \\subsection bodies with compact auto-generated
    paragraphs + \\input of the results-tables and results-figures,
    wrapped in a pair of generated markers so re-runs are idempotent.
  * fill the abstract's \\tbdline with the headline numbers when a DFM
    result exists.

Markers used:
  % BEGIN-AUTO-subsec_elasticities ... % END-AUTO-subsec_elasticities
  % BEGIN-AUTO-subsec_composite ... etc.

If the marker pair is absent, we insert the block immediately after the
matching "\\subsection{...}\\n\\label{...}" line.

Running the script is safe on each pipeline refresh.
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from _common import abs_path, load_env  # noqa: E402

load_env()


PAPER = abs_path("paper/fires_lights_smog.tex")


def _safe(p: Path) -> dict:
    if p.exists():
        try:
            return json.loads(p.read_text())
        except Exception:
            return {}
    return {}


def _fmt(x, fmt="{:+.2f}") -> str:
    if x is None:
        return "---"
    try:
        return fmt.format(float(x))
    except Exception:
        return "---"


def _block(marker: str, body: str) -> str:
    return (f"\n% BEGIN-AUTO-{marker}\n{body.strip()}\n% END-AUTO-{marker}\n")


def _replace_or_insert(text: str, marker: str, anchor_label: str, block: str) -> str:
    start = f"% BEGIN-AUTO-{marker}"
    end = f"% END-AUTO-{marker}"
    replacement = block.strip()
    if start in text and end in text:
        pat = re.compile(rf"{re.escape(start)}.*?{re.escape(end)}", flags=re.S)
        return pat.sub(lambda _m: replacement, text)
    # Otherwise insert after the \label line. Use lambda to avoid backreference
    # interpretation of backslashes in `block`.
    pat = re.compile(rf"\\label\{{{re.escape(anchor_label)}\}}", flags=re.M)
    m = pat.search(text)
    if not m:
        return text
    anchor_end = m.end()
    return text[:anchor_end] + block + text[anchor_end:]


def body_elasticities() -> str:
    v = _safe(abs_path("data/satellite/elasticity_viirs.json"))
    f = _safe(abs_path("data/satellite/elasticity_vnf.json"))
    n = _safe(abs_path("data/satellite/elasticity_no2.json"))
    lines = [r"\input{tables/elasticities.tex}", ""]
    parts = []
    for name, d, ref in [
        ("VIIRS", v, r"\citet{hsw2012}"),
        ("VNF", f, r"\citet{do_etal2018}"),
        ("NO$_2$", n, r"\citet{bauwens2020}"),
    ]:
        if d.get("status") == "ok":
            parts.append(rf"\claim{{{name}}} Estimated $\hat\beta = {_fmt(d['beta'],'{:+.3f}')}$ "
                         rf"(SE {_fmt(d['se'],'{:.3f}')}, $n={d['n']}$) vs.\ {ref}.")
        else:
            parts.append(rf"\claim{{{name}}} Data required to estimate ({d.get('status')}).")
    return "\n".join(lines + parts)


def _tbd_fig_or_real(fig_path: str, caption: str, label: str) -> str:
    """Render a figure if all required inputs are non-empty; otherwise a TBD box."""
    fig_abs = abs_path(fig_path)
    dfm = _safe(abs_path("data/satellite/dfm_result.json"))
    if fig_abs.exists() and dfm.get("status") == "ok":
        return (rf"\begin{{figure}}[H]\centering\includegraphics[width=0.95\linewidth]"
                rf"{{{fig_path}}}\caption{{{caption}}}\label{{{label}}}\end{{figure}}")
    return rf"\tbdline{{figure to be inserted after empirical estimation ({label})}}"


def body_composite() -> str:
    dfm = _safe(abs_path("data/satellite/dfm_result.json"))
    if dfm.get("status") != "ok":
        return (r"\tbdline{figure to be inserted after empirical estimation "
                r"(fig:factor)}")
    return "\n".join([
        _tbd_fig_or_real("figures/factor_and_bbq.pdf",
                         "Satellite factor and Bry--Boschan turning points.",
                         "fig:factor_bbq"),
        rf"\claim{{Fit summary}} Two-factor DFM (urban + extractive) "
        rf"with AR(2) factor dynamics and idiosyncratic AR(1) innovations, "
        rf"log-likelihood ${_fmt(dfm['log_likelihood'],'{:.1f}')}$, "
        rf"$n={dfm['n_obs']}$ monthly observations. Loadings and factor-"
        rf"decomposition variance shares in the replication archive.",
    ])


def body_dating() -> str:
    d = _safe(abs_path("data/satellite/recession_dating.json"))
    bbq = d.get("bbq", {})
    ms = d.get("markov_switching", {})
    dfm_ok = _safe(abs_path("data/satellite/dfm_result.json")).get("status") == "ok"
    if not dfm_ok or bbq.get("status") != "ok":
        return (r"\tbdline{figure to be inserted after empirical estimation "
                r"(fig:markov)}")
    lines = [
        _tbd_fig_or_real("figures/markov_probability.pdf",
                         "Hamilton two-state Markov-switching recession probability "
                         "on the first difference of the satellite factor.",
                         "fig:markov"),
        rf"\claim{{BBQ turning points}} Peaks: "
        rf"{', '.join(bbq.get('peaks', [])) or '---'}. Troughs: "
        rf"{', '.join(bbq.get('troughs', [])) or '---'}.",
    ]
    if ms.get("status") == "ok":
        n_rec = sum(1 for p in ms["p_recession"] if p > 0.5)
        lines.append(
            rf"\claim{{Markov-switching}} Recession-regime mean "
            rf"$\mu_r={_fmt(ms['recession_mean'],'{:+.3f}')}$ "
            rf"per month, expansion $\mu_e={_fmt(ms['expansion_mean'],'{:+.3f}')}$. "
            rf"Months with $P(\mathrm{{rec}})>0.5$: {n_rec}."
        )
    return "\n".join(lines)


def body_ine_comparison() -> str:
    bench = _safe(abs_path("data/satellite/benchmark_ine.json"))
    if bench.get("status") != "ok":
        return r"\tbdline{INE benchmark regression requires IGAE panel \u2265 24 vintages (expected March 2028).}"
    return "\n".join([
        r"\input{tables/ine_vs_satellite.tex}", "",
        _tbd_fig_or_real("figures/ine_vs_satellite.pdf",
                         "Satellite factor vs INE GDP growth.",
                         "fig:ine_vs_sat"),
    ])


def body_manipulation() -> str:
    m = _safe(abs_path("data/satellite/manipulation_tests.json"))
    any_ok = any(
        isinstance(v, dict) and v.get("status") == "ok"
        for v in m.values()
    )
    if not any_ok:
        return (r"\tbdline{Test 1 (sectoral triangulation), Test 2 (Nov-2025 INE "
                r"leadership discontinuity), and Test 3 (external-forecaster residual) "
                r"require YPFB field-month production, INE hydrocarbon value-added, "
                r"and consensus external forecasts. Results to be inserted after "
                r"official series are loaded.}")
    return r"\input{tables/manipulation.tex}"


def body_channels() -> str:
    dfm = _safe(abs_path("data/satellite/dfm_result.json"))
    if dfm.get("status") != "ok":
        return (r"\tbdline{Channel-decomposition table to be inserted after "
                r"two-factor DFM estimation.}")
    return (r"\claim{Channel decomposition} Contribution of each stream to the "
            r"monthly factor is recovered from the estimated loadings "
            r"$\lambda_i$ and reported in the replication archive.")


def body_abstract_headline() -> str:
    dfm = _safe(abs_path("data/satellite/dfm_result.json"))
    disag = _safe(abs_path("data/satellite/igae_disagreement.json"))
    if dfm.get("status") == "ok" and dfm.get("factor_z"):
        f = dfm["factor_z"]
        idx = dfm["factor_index"]
        lo = min(f); lo_m = idx[f.index(lo)]
        latest = f[-1]; latest_m = idx[-1]
        gap = disag.get("gap")
        return (rf"Headline: the satellite factor bottoms at $z={_fmt(lo)}$ "
                rf"in {lo_m[:7]}; latest ({latest_m[:7]}) at $z={_fmt(latest)}$. "
                + (rf"Residual vs INE IGAE: ${_fmt(gap,'{:.2f}')}\sigma$."
                   if gap is not None else ""))
    return ""


def main() -> None:
    if not PAPER.exists():
        print(f"[warn] paper not found at {PAPER}")
        return
    text = PAPER.read_text()

    patches = [
        ("subsec_elasticities", "subsec:elasticities", body_elasticities()),
        ("subsec_composite", "subsec:composite", body_composite()),
        ("subsec_dating", "subsec:dating", body_dating()),
        ("subsec_ine_comparison", "subsec:ine_comparison", body_ine_comparison()),
        ("subsec_manipulation", "subsec:manipulation", body_manipulation()),
        ("subsec_channels", "subsec:channels", body_channels()),
    ]
    for marker, label, body in patches:
        block = _block(marker, body)
        text = _replace_or_insert(text, marker, label, block)

    # Abstract headline. First run: replace the \tbdline placeholder with a
    # marker-wrapped block; subsequent runs: overwrite the marker block.
    headline = body_abstract_headline()
    if headline:
        wrapped = f"% BEGIN-AUTO-headline {headline} % END-AUTO-headline"
        if "BEGIN-AUTO-headline" in text:
            text = re.sub(
                r"% BEGIN-AUTO-headline.*?% END-AUTO-headline",
                lambda _m: wrapped,
                text, count=1, flags=re.S,
            )
        else:
            text = re.sub(
                r"\\tbdline\{Headline result[^}]*\}",
                lambda _m: wrapped,
                text, count=1,
            )

    PAPER.write_text(text)
    print(f"[ok] patched {PAPER}")


if __name__ == "__main__":
    main()
