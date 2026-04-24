"""V3 figure builder — produces the 5 main + 2 appendix figures declared
in the brief.

Rule zero compliance: if a required input is missing, we do NOT render
a "TBD stream" panel. Instead we produce a blank placeholder PDF with a
single line of text "Figure pending: requires <input>" and log to
/figures/pending.log.

Rule two compliance: every time-series axis carries
    ax.set_xlim(pd.Timestamp("2012-04-01"), pd.Timestamp("2026-05-01"))
explicitly (except NDVI which can start 2013-01).
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import yaml

HERE = Path(__file__).resolve().parent
V3 = HERE.parent
REPO = V3.parents[1]
sys.path.insert(0, str(REPO / "scripts" / "figures"))

from lalinterna_style import PALETTE, LINE, apply_style, save_fig  # noqa: E402

apply_style(use_tex=False)

PROCESSED = V3 / "data" / "processed"
ESTIMATES = V3 / "data" / "estimates"
OUT_PDF = V3 / "figures" / "pdf"
OUT_PDF.mkdir(parents=True, exist_ok=True)
PENDING = V3 / "figures" / "pending.log"

X_MIN = pd.Timestamp("2012-04-01")
X_MAX = pd.Timestamp("2026-05-01")

PAZ_INAUG = pd.Timestamp("2025-11-08")
FUEL_BREAK = pd.Timestamp("2025-12-01")
COVID = pd.Timestamp("2020-03-01")

INE_CONTRACTIONS = [
    (pd.Timestamp("2020-04-01"), pd.Timestamp("2020-09-30")),
    (pd.Timestamp("2024-01-01"), pd.Timestamp("2025-12-31")),
]

SPATIAL = yaml.safe_load((V3 / "config" / "spatial.yaml").read_text())


def _log_pending(track: str, reason: str) -> None:
    ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
    with open(PENDING, "a") as f:
        f.write(f"{ts}  {track}  {reason}\n")


def _placeholder_pdf(path: Path, message: str) -> None:
    """Blank PDF with a single line 'Figure pending: requires <input>'.

    No axes, no colors, no plot-looking content. The PDF exists so
    pdflatex's graphicx does not error on missing files; the reader
    instantly sees the figure is not produced."""
    fig, ax = plt.subplots(figsize=(6.0, 3.5))
    ax.axis("off")
    ax.text(0.5, 0.5, f"Figure pending: requires {message}",
            ha="center", va="center", fontsize=10,
            color=PALETTE["neutral"], transform=ax.transAxes)
    save_fig(fig, path)
    plt.close(fig)


def _apply_time_axis(ax, start=None, end=None) -> None:
    ax.set_xlim(start or X_MIN, end or X_MAX)


def _zscore(s: pd.Series, baseline_end: pd.Timestamp = pd.Timestamp("2019-12-31")) -> pd.Series:
    base = s[s.index <= baseline_end]
    mu, sd = base.mean(), base.std(ddof=1)
    if sd is None or sd == 0:
        return s - mu
    return (s - mu) / sd


# -------- Fig 1: four satellite streams -----------------------------

def fig1_four_streams() -> None:
    missing_streams: list[str] = []

    # Collect available composites
    panels: list[tuple[str, pd.Series]] = []

    # VIIRS — population-weighted composite of monthly SOL z-score
    vp = PROCESSED / "viirs_dnb_monthly.parquet"
    if vp.exists():
        df = pd.read_parquet(vp)
        df = df.dropna(subset=["sol"])
        if "low_coverage_flag" in df.columns:
            df = df[~df["low_coverage_flag"].astype(bool)]
        pop = {c["name"]: c["population"] for c in SPATIAL["urban_buffers"]}
        df["pop"] = df["buffer_name"].map(pop).fillna(0)
        df["log_sol"] = np.log(df["sol"].clip(lower=1))
        # Per buffer: z vs 2013-01..2019-12 baseline
        def _zb(g: pd.DataFrame) -> pd.Series:
            base = g[g["date"] <= pd.Timestamp("2019-12-31")]
            mu, sd = base["log_sol"].mean(), base["log_sol"].std(ddof=1)
            if sd == 0 or pd.isna(sd):
                return g["log_sol"] - mu
            return (g["log_sol"] - mu) / sd
        df["z"] = df.groupby("buffer_name", group_keys=False).apply(_zb)
        comp = df.groupby("date").apply(
            lambda g: np.average(g["z"], weights=g["pop"])
            if g["pop"].sum() > 0 else np.nan,
            include_groups=False).rename("viirs_z")
        panels.append(("VIIRS DNB (11 urban buffers, pop-weighted z)", comp))
    else:
        missing_streams.append("VIIRS DNB")

    # VNF — missing per D9
    if (PROCESSED / "vnf_chaco_monthly.parquet").exists():
        v = pd.read_parquet(PROCESSED / "vnf_chaco_monthly.parquet")
        total = v[v["field"] == "TOTAL_CHACO"].set_index("date")["rh_mw_sum"]
        log_total = np.log(total.clip(lower=1))
        panels.append((r"VNF Chaco (6 fields, $\Sigma\,$RH z-score)",
                       _zscore(log_total)))
    else:
        missing_streams.append("VNF Chaco")

    # NO2 — two-metro mean z vs 2019 baseline
    nt = PROCESSED / "s5p_no2_monthly.parquet"
    if nt.exists():
        n = pd.read_parquet(nt)
        m = n[n["roi"].isin(["la_paz_el_alto", "santa_cruz"])]
        def _zm(g: pd.DataFrame) -> pd.Series:
            base = g[(g["date"] >= pd.Timestamp("2019-01-01")) &
                     (g["date"] <= pd.Timestamp("2019-12-31"))]
            mu, sd = base["no2_tropos_col"].mean(), base["no2_tropos_col"].std(ddof=1)
            if sd == 0 or pd.isna(sd):
                return g["no2_tropos_col"] - mu
            return (g["no2_tropos_col"] - mu) / sd
        m = m.copy()
        m["z"] = m.groupby("roi", group_keys=False).apply(_zm)
        comp = m.groupby("date")["z"].mean().rename("no2_z")
        panels.append((r"TROPOMI NO$_2$ (La Paz + Santa Cruz mean z)", comp))
    else:
        missing_streams.append("TROPOMI NO2")

    # NDVI — zone GVA-weighted monthly z
    np_p = PROCESSED / "s2_ndvi_monthly.parquet"
    if np_p.exists():
        d = pd.read_parquet(np_p).dropna(subset=["ndvi_median"])
        w = {z["name"]: z["gva_weight"] for z in SPATIAL["cropland_zones"]}
        d["w"] = d["zone"].map(w).fillna(0)
        # Per zone: month-of-year baseline 2013-2019
        d["month"] = d["date"].dt.month
        base = d[(d["date"] >= pd.Timestamp("2013-01-01")) &
                 (d["date"] <= pd.Timestamp("2019-12-31"))]
        seas = base.groupby(["zone", "month"])["ndvi_median"].mean().rename("seas")
        sd = base.groupby("zone")["ndvi_median"].std(ddof=1).rename("sd")
        d = d.merge(seas, on=["zone", "month"]).merge(sd, on="zone")
        d["z"] = (d["ndvi_median"] - d["seas"]) / d["sd"]
        comp = d.groupby("date").apply(
            lambda g: np.average(g["z"], weights=g["w"])
            if g["w"].sum() > 0 else np.nan,
            include_groups=False).rename("ndvi_z")
        panels.append(("Sentinel-2 NDVI (5 cropland zones, GVA-weighted z)", comp))
    else:
        missing_streams.append("Sentinel-2 NDVI")

    if not panels:
        _log_pending("F1", "no satellite streams available")
        _placeholder_pdf(OUT_PDF / "fig1_four_streams.pdf",
                         "all four satellite streams")
        return

    n = len(panels)
    fig, axes = plt.subplots(n, 1, figsize=(6.0, 2.0 * n), sharex=True)
    if n == 1:
        axes = [axes]
    for ax, (title, series) in zip(axes, panels):
        series = series.dropna().sort_index()
        ax.plot(series.index, series.values,
                color=PALETTE["neutral"], lw=LINE["primary"])
        pos = series.values >= 0
        ax.fill_between(series.index, series.values, 0, where=pos,
                         color=PALETTE["positive"], alpha=0.15, linewidth=0)
        ax.fill_between(series.index, series.values, 0, where=~pos,
                         color=PALETTE["negative"], alpha=0.15, linewidth=0)
        ax.axhline(0, color="black", lw=0.4)
        for a, b in INE_CONTRACTIONS:
            ax.axvspan(a, b, color=PALETTE["neutral"], alpha=0.08, linewidth=0)
        ax.axvline(PAZ_INAUG, color=PALETTE["tertiary"], ls="--", lw=0.6)
        ax.axvline(FUEL_BREAK, color=PALETTE["tertiary"], ls="--", lw=0.6)
        ax.set_title(title, loc="left", fontsize=9)
        ax.set_ylabel("z")
        _apply_time_axis(ax)
    fig.tight_layout()
    save_fig(fig, OUT_PDF / "fig1_four_streams.pdf")
    plt.close(fig)
    if missing_streams:
        _log_pending("F1", "rendered with streams missing: "
                           + ", ".join(missing_streams))
    print(f"[F1] fig1_four_streams.pdf  (panels={len(panels)})")


# -------- Fig 2: elasticities vs literature -------------------------

def fig2_elasticities() -> None:
    litbench = {
        "VIIRS DNB":        ("HSW 2012 / Beyer-Hu-Yao",    0.30, 0.05),
        "VNF":              (r"Do et al.\ 2018",           1.00, 0.20),
        "TROPOMI NO$_2$":   (r"Bauwens 2020",              0.35, 0.15),
        "Sentinel-2 NDVI":  (r"Johnson 2014",              0.40, 0.15),
    }
    ep = ESTIMATES / "elasticities.parquet"
    est = pd.read_parquet(ep) if ep.exists() else pd.DataFrame()
    est_by_stream = {r["stream"]: r for _, r in est.iterrows()}

    rows = list(litbench.keys())
    fig, ax = plt.subplots(figsize=(6.0, 4.0))
    ax.axvline(0, color="black", lw=0.4)
    for i, name in enumerate(rows):
        lb_name, lb_beta, lb_se = litbench[name]
        ax.errorbar(lb_beta, i + 0.2, xerr=1.96 * lb_se,
                     fmt="D", mfc="white", mec=PALETTE["neutral"],
                     ecolor=PALETTE["neutral"], ms=6, capsize=3)
        est_key = "TROPOMI NO$_2$" if name == "TROPOMI NO$_2$" else name
        # match on .str.contains for the stream label
        match = next((r for s, r in est_by_stream.items() if name.split()[0] in s),
                     None)
        if match is not None and pd.notna(match.get("beta")):
            xerr_lo, xerr_hi = (1.96 * match["se"] if pd.notna(match.get("se"))
                                else 0, 1.96 * match["se"]
                                if pd.notna(match.get("se")) else 0)
            ax.errorbar(match["beta"], i - 0.2,
                         xerr=1.96 * match["se"] if pd.notna(match["se"]) else None,
                         fmt="o", mfc=PALETTE["negative"],
                         mec=PALETTE["negative"], ecolor=PALETTE["negative"],
                         ms=6, capsize=3)
            ax.text(match["beta"] + 0.05, i - 0.2,
                     rf"  $n={match['n']}$", fontsize=7,
                     color=PALETTE["neutral"], va="center")
        else:
            ax.text(lb_beta + 1.96 * lb_se + 0.15, i - 0.2, "pending",
                     fontsize=7, color=PALETTE["neutral"], va="center",
                     style="italic")

    ax.set_yticks(list(range(len(rows))))
    ax.set_yticklabels(rows)
    ax.invert_yaxis()
    ax.set_xlabel(r"$\hat\beta$")
    ax.set_title(r"Single-series elasticities: Bolivia vs literature benchmarks",
                 loc="left", fontsize=10)
    # Legend via dummy handles (no frame)
    h1 = plt.Line2D([], [], color=PALETTE["neutral"], marker="D", mfc="white",
                    lw=0, ms=6, label="Literature (95\\% CI)")
    h2 = plt.Line2D([], [], color=PALETTE["negative"], marker="o", lw=0, ms=6,
                    label="Bolivia estimate (95\\% CI)")
    ax.legend(handles=[h1, h2], loc="lower right", frameon=False,
              fontsize=8)
    fig.tight_layout()
    save_fig(fig, OUT_PDF / "fig2_elasticities.pdf")
    plt.close(fig)
    print("[F2] fig2_elasticities.pdf")


# -------- Fig 3: composite vs INE -----------------------------------

def fig3_composite_vs_ine() -> None:
    # DFM composite not produced (E2 blocked). Per brief rule zero: do not
    # render "composite not produced" text; produce placeholder PDF.
    _log_pending("F3", "E2 DFM composite not produced; per brief, "
                       "not substituting CI fallback.")
    _placeholder_pdf(OUT_PDF / "fig3_composite_vs_ine.pdf",
                     "two-factor DFM composite (E2)")
    print("[F3] placeholder (E2 missing)")


# -------- Fig 4: sectoral decomposition -----------------------------

def fig4_sectoral_decomposition() -> None:
    _log_pending("F4", "E2 DFM loadings not produced; "
                       "INE sectoral quarterly Parquet loaded but top panel "
                       "requires DFM channel decomposition.")
    _placeholder_pdf(OUT_PDF / "fig4_sectoral_decomposition.pdf",
                     "two-factor DFM loadings (E2) for the top panel")
    print("[F4] placeholder (E2 missing)")


# -------- Fig 5: manipulation verdict (monthly VNF–YPFB required) ---

def fig5_manipulation_verdict() -> None:
    if not (PROCESSED / "vnf_chaco_monthly.parquet").exists() \
       or not (PROCESSED / "ypfb_field_monthly.parquet").exists():
        _log_pending("F5", "requires monthly VNF Chaco + monthly YPFB "
                           "field-month. Per brief, do not substitute "
                           "annual WB-GGFR.")
        _placeholder_pdf(OUT_PDF / "fig5_manipulation_verdict.pdf",
                         "monthly VNF Chaco (D9) + monthly YPFB (D4)")
        print("[F5] placeholder (D4 and D9 missing)")
        return
    # Full implementation lives for when both Parquets exist.
    _log_pending("F5", "both Parquets present; implementation TBD.")
    _placeholder_pdf(OUT_PDF / "fig5_manipulation_verdict.pdf",
                     "manipulation-verdict implementation wiring")


# -------- Fig A1: spatial coverage ----------------------------------

def figA1_spatial_coverage() -> None:
    """Map with urban buffers, Chaco fields, TROPOMI ROIs, and NDVI zones.
    No GEE basemap in this pass (GEE export requires the Haiti service
    account and an export task); we render the boundary + overlay only.
    """
    fig, axes = plt.subplots(1, 3, figsize=(10.0, 4.5))
    for ax in axes:
        ax.set_xlim(-72, -57.0); ax.set_ylim(-23.5, -9.5)
        ax.set_aspect("equal")
        ax.grid(True, alpha=0.2, lw=0.3)
        ax.set_xlabel("longitude", fontsize=7)
        ax.set_ylabel("latitude", fontsize=7)
        # 2-degree graticule (brief says graticule present)
        for x in range(-72, -56, 2):
            ax.axvline(x, color=PALETTE["neutral"], lw=0.15, alpha=0.3)
        for y in range(-23, -9, 2):
            ax.axhline(y, color=PALETTE["neutral"], lw=0.15, alpha=0.3)

    # Left: urban buffers with full labels
    for city in SPATIAL["urban_buffers"]:
        circ = plt.Circle((city["lon"], city["lat"]),
                            city["radius_km"] / 111.0,
                            fill=False, color=PALETTE["positive"], lw=0.8)
        axes[0].add_patch(circ)
        axes[0].annotate(city["label"],
                          (city["lon"], city["lat"]),
                          fontsize=6, color=PALETTE["neutral"],
                          xytext=(3, 3), textcoords="offset points")
    axes[0].set_title("VIIRS DNB urban buffers", loc="left", fontsize=9)

    # Center: Chaco bbox + fields
    bbox = SPATIAL["chaco"]["bbox"]
    axes[1].add_patch(mpatches.Rectangle(
        (bbox[0], bbox[1]), bbox[2] - bbox[0], bbox[3] - bbox[1],
        fill=False, color=PALETTE["negative"], lw=0.8))
    for fld in SPATIAL["chaco"]["fields"]:
        axes[1].plot(fld["lon"], fld["lat"], "*",
                      color=PALETTE["negative"], ms=9)
        axes[1].annotate(fld["label"], (fld["lon"], fld["lat"]),
                          fontsize=6, color=PALETTE["neutral"],
                          xytext=(5, 3), textcoords="offset points")
    axes[1].set_title("Chaco basin and seven gas fields", loc="left", fontsize=9)

    # Right: TROPOMI ROIs + NDVI zones
    for roi in SPATIAL["tropomi_rois"]:
        axes[2].add_patch(mpatches.Rectangle(
            (min(roi["nw_lon"], roi["se_lon"]),
             min(roi["nw_lat"], roi["se_lat"])),
            abs(roi["se_lon"] - roi["nw_lon"]),
            abs(roi["nw_lat"] - roi["se_lat"]),
            fill=False, color=PALETTE["tertiary"], lw=0.8))
    for zone in SPATIAL["cropland_zones"]:
        c = plt.Circle((zone["lon"], zone["lat"]),
                         zone["radius_km"] / 111.0,
                         fill=True, color=PALETTE["positive"],
                         alpha=0.15, lw=0.5)
        axes[2].add_patch(c)
        axes[2].annotate(zone["label"],
                          (zone["lon"], zone["lat"]),
                          fontsize=6, color=PALETTE["neutral"],
                          xytext=(3, 3), textcoords="offset points")
    axes[2].set_title(r"NO$_2$ ROIs and NDVI cropland zones",
                       loc="left", fontsize=9)

    fig.tight_layout()
    save_fig(fig, OUT_PDF / "figA1_spatial_coverage.pdf")
    plt.close(fig)
    print("[FA1] figA1_spatial_coverage.pdf")


# -------- Fig A2: field-level calibration ---------------------------

def figA2_field_calibration() -> None:
    _log_pending("FA2", "requires D4 (YPFB field-month) and D9 "
                        "(VNF Chaco monthly). Cannot be produced.")
    _placeholder_pdf(OUT_PDF / "figA2_field_calibration.pdf",
                     "monthly YPFB field production (D4) and "
                     "monthly VNF per field (D9)")
    print("[FA2] placeholder (D4 and D9 missing)")


def main() -> None:
    fig1_four_streams()
    fig2_elasticities()
    fig3_composite_vs_ine()
    fig4_sectoral_decomposition()
    fig5_manipulation_verdict()
    figA1_spatial_coverage()
    figA2_field_calibration()


if __name__ == "__main__":
    main()
