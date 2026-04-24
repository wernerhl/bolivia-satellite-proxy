"""Generate all paper v2 figures (Track H).

Main text: fig1_four_streams, fig2_elasticities, fig3_composite_vs_ine,
           fig4_sectoral_decomposition, fig5_manipulation_verdict.
Appendix:  figA1_spatial_coverage, figA2_field_calibration.

Outputs under figures/pdf/ and mirrors under figures/png/. Graceful
TBD handling when underlying data is absent.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts" / "figures"))

from _common import abs_path, load_env, paths  # noqa: E402
from lalinterna_style import (  # noqa: E402
    PALETTE, LINE, apply_style, recession_band, reference_line, save_fig,
)

load_env()
apply_style(use_tex=False)


OUT_PDF = ROOT / "paper" / "v2" / "figures" / "pdf"
OUT_PNG = ROOT / "paper" / "v2" / "figures" / "png"
OUT_PDF.mkdir(parents=True, exist_ok=True)
OUT_PNG.mkdir(parents=True, exist_ok=True)


# INE-reported contraction bands (quarterly)
CONTRACTIONS = [
    (pd.Timestamp("2020-04-01"), pd.Timestamp("2020-09-30")),
    (pd.Timestamp("2024-01-01"), pd.Timestamp("2025-12-31")),
]
PAZ_INAUG = pd.Timestamp("2025-11-08")
FUEL_BREAK = pd.Timestamp("2025-12-01")
COVID = pd.Timestamp("2020-03-01")
PARALLEL_RATE = pd.Timestamp("2023-02-01")


# ---------- Helpers --------------------------------------------------

def _load_ci() -> pd.DataFrame:
    p = paths()
    df = pd.read_csv(abs_path(p["data"]["ci"]), parse_dates=["date"])
    return df.dropna(subset=["ci"]).sort_values("date")


def _load_ine_gdp() -> pd.DataFrame | None:
    q = abs_path("data/official/ine_gdp_quarterly.csv")
    if not q.exists():
        return None
    df = pd.read_csv(q, parse_dates=["date"]).sort_values("date")
    df["growth_yoy"] = np.log(df["gdp_real"]).diff(4)
    return df


def _tbd_panel(ax, reason: str) -> None:
    ax.text(0.5, 0.5, f"TBD\n{reason}",
            transform=ax.transAxes, ha="center", va="center",
            fontsize=10, color=PALETTE["neutral"], alpha=0.65)
    ax.set_xticks([]); ax.set_yticks([])


# ---------- Fig 1 — four satellite streams ---------------------------

def fig1_four_streams() -> None:
    p = paths()
    fig, axes = plt.subplots(4, 1, figsize=(6.0, 7.0), sharex=True)

    def _stream(ax, df, y_col, title, ymin=None, ymax=None):
        if df is None or df.empty:
            _tbd_panel(ax, "stream not yet available")
            ax.set_title(title, loc="left", fontsize=9)
            return
        ax.plot(df["date"], df[y_col], color=PALETTE["neutral"],
                lw=LINE["primary"])
        pos = df[y_col] >= 0
        ax.fill_between(df["date"], df[y_col], 0, where=pos,
                         color=PALETTE["positive"], alpha=0.15, linewidth=0)
        ax.fill_between(df["date"], df[y_col], 0, where=~pos,
                         color=PALETTE["negative"], alpha=0.15, linewidth=0)
        ax.axhline(0, color="black", lw=0.4)
        for a, b in CONTRACTIONS:
            recession_band(ax, a, b)
        ax.axvline(PAZ_INAUG, color=PALETTE["tertiary"], ls="--", lw=0.6)
        ax.axvline(FUEL_BREAK, color=PALETTE["tertiary"], ls="--", lw=0.6)
        ax.set_title(title, loc="left", fontsize=9)
        if ymin is not None: ax.set_ylim(ymin, ymax)

    # VIIRS composite z from CI file
    ci = _load_ci()
    _stream(axes[0], ci[["date", "viirs_z"]].dropna().rename(
        columns={"viirs_z": "y"}).rename(columns={"y": "viirs_z"}),
        "viirs_z", "VIIRS DNB sum-of-lights (11 urban buffers)")

    # VNF
    vnf = abs_path(p["data"]["vnf_anomaly"])
    vnf_df = None
    if vnf.exists():
        vnf_df = pd.read_csv(vnf, parse_dates=["date"])
        if vnf_df.empty or "rh_anomaly_z" not in vnf_df.columns:
            vnf_df = None
        else:
            vnf_df = vnf_df.rename(columns={"rh_anomaly_z": "y"})
    _stream(axes[1], vnf_df, "y", "VIIRS Nightfire (Chaco, 6 fields)")

    # NO2
    no2 = pd.read_csv(abs_path(p["data"]["s5p_anomaly"]), parse_dates=["date"])
    no2m = no2[no2["roi"].isin(["la_paz_el_alto", "santa_cruz"])]
    no2c = no2m.groupby("date", as_index=False)["z_vs_2019"].mean()
    _stream(axes[2], no2c.rename(columns={"z_vs_2019": "y"}), "y",
            r"TROPOMI NO$_2$ (La Paz--El Alto + Santa Cruz)")

    # NDVI
    nd = abs_path(p["data"]["s2_ndvi_anomaly"])
    nd_df = None
    if nd.exists():
        nd_df = pd.read_csv(nd, parse_dates=["date"])
        if nd_df.empty or "anomaly_z" not in nd_df.columns:
            nd_df = None
        else:
            # GVA-weighted composite
            from _common import ndvi_zones
            w = {z["name"]: z["gva_weight"] for z in ndvi_zones()}
            nd_df["w"] = nd_df["zone"].map(w).fillna(0)
            agg = nd_df.dropna(subset=["anomaly_z"]).groupby("date").apply(
                lambda g: np.average(g["anomaly_z"], weights=g["w"])
                if g["w"].sum() > 0 else np.nan,
                include_groups=False).rename("y").reset_index()
            nd_df = agg
    _stream(axes[3], nd_df, "y", "Sentinel-2 NDVI (5 cropland zones)")

    for ax in axes:
        ax.set_ylabel("z-score")
    axes[-1].set_xlabel("")
    fig.tight_layout()
    save_fig(fig, OUT_PDF / "fig1_four_streams.pdf")
    plt.close(fig)
    print("[ok] fig1_four_streams.pdf")


# ---------- Fig 2 — elasticities vs literature -----------------------

def fig2_elasticities() -> None:
    # Benchmarks from literature
    rows = [
        ("VIIRS", "hsw2012", 0.30, 0.05),
        ("VNF",   "do_etal2018", 1.00, 0.20),
        (r"NO$_2$","bauwens2020", 0.35, 0.15),
        ("NDVI",  "johnson2014",  0.40, 0.15),
    ]
    fig, ax = plt.subplots(figsize=(6.0, 4.0))
    ypos = np.arange(len(rows))
    # Bolivia estimates from JSON when available
    estimates = {}
    for name, key in [("VIIRS", "viirs"), ("VNF", "vnf"),
                      (r"NO$_2$", "no2"), ("NDVI", "ndvi")]:
        j = abs_path(f"data/satellite/elasticity_{key}.json")
        if j.exists():
            d = json.loads(j.read_text())
            if d.get("status") == "ok":
                estimates[name] = (d["beta"], d["se"], d.get("n", "--"))

    for i, (name, _, lit_b, lit_se) in enumerate(rows):
        # Literature benchmark (hollow diamond + whiskers)
        ax.errorbar(lit_b, i + 0.15, xerr=1.96 * lit_se,
                    fmt="D", mfc="white", mec=PALETTE["neutral"],
                    ecolor=PALETTE["neutral"], ms=6, capsize=3,
                    label="Literature" if i == 0 else None)
        if name in estimates:
            b, se, n = estimates[name]
            ax.errorbar(b, i - 0.15, xerr=1.96 * se,
                        fmt="o", mfc=PALETTE["negative"],
                        mec=PALETTE["negative"], ecolor=PALETTE["negative"],
                        ms=6, capsize=3,
                        label="Bolivia" if i == 0 else None)
            ax.text(ax.get_xlim()[1] * 0.98 if False else lit_b + 2 * lit_se + 0.25,
                    i, f"n={n}", va="center", ha="left", fontsize=7,
                    color=PALETTE["neutral"])
        else:
            ax.text(lit_b, i - 0.15, "  Bolivia est. pending", va="center",
                    ha="left", fontsize=7, color=PALETTE["neutral"], alpha=0.7)

    ax.axvline(0, color="black", lw=0.4)
    ax.set_yticks(ypos)
    ax.set_yticklabels([r[0] for r in rows])
    ax.invert_yaxis()
    ax.set_xlabel(r"$\hat\beta$")
    ax.set_title("Satellite-to-activity elasticities: Bolivia vs literature",
                 loc="left", fontsize=10)
    ax.legend(loc="lower right", frameon=False)
    fig.tight_layout()
    save_fig(fig, OUT_PDF / "fig2_elasticities.pdf")
    plt.close(fig)
    print("[ok] fig2_elasticities.pdf")


# ---------- Fig 3 — the composite and INE ----------------------------

def fig3_composite_vs_ine() -> None:
    ci = _load_ci()
    ine = _load_ine_gdp()

    fig, ax = plt.subplots(figsize=(6.5, 4.0))
    ax.plot(ci["date"], ci["ci"], color=PALETTE["positive"],
            lw=LINE["primary"], label="Satellite composite (z)")
    # 68/95 bands using rolling MAD
    if len(ci) > 24:
        rolling = ci.set_index("date")["ci"].rolling(12, min_periods=6)
        mu, sd = rolling.mean(), rolling.std()
        ax.fill_between(mu.index, mu - sd, mu + sd,
                         color=PALETTE["positive"], alpha=0.25, linewidth=0)
        ax.fill_between(mu.index, mu - 2 * sd, mu + 2 * sd,
                         color=PALETTE["positive"], alpha=0.10, linewidth=0)

    ax.axhline(0, color="black", lw=0.4)
    for a, b in CONTRACTIONS:
        recession_band(ax, a, b)

    # INE on right axis
    if ine is not None and ine["growth_yoy"].notna().any():
        ax2 = ax.twinx()
        ine_plot = ine.dropna(subset=["growth_yoy"])
        ax2.step(ine_plot["date"], ine_plot["growth_yoy"] * 100,
                 where="post", color=PALETTE["negative"],
                 lw=LINE["comparison"], label="INE GDP y/y (%)")
        ax2.axhline(0, color=PALETTE["negative"], lw=0.3, ls=":")
        ax2.set_ylabel("INE GDP growth y/y (%)", color=PALETTE["negative"])
        ax2.tick_params(axis="y", colors=PALETTE["negative"])
        ax2.spines["right"].set_visible(True)
        ax2.spines["right"].set_color(PALETTE["negative"])

    for d, lbl in [(COVID, "COVID"), (PARALLEL_RATE, "parallel rate"),
                   (PAZ_INAUG, "Paz + INE director"),
                   (FUEL_BREAK, "fuel subsidy end")]:
        ax.axvline(d, color=PALETTE["neutral"], ls="--", lw=0.6, alpha=0.6)
        ax.text(d, ax.get_ylim()[1] * 0.92, " " + lbl, rotation=90,
                fontsize=7, color=PALETTE["neutral"], ha="left", va="top")

    ax.set_ylabel("Satellite composite (z)", color=PALETTE["positive"])
    ax.tick_params(axis="y", colors=PALETTE["positive"])
    ax.set_title("Monthly satellite composite and quarterly INE GDP",
                 loc="left", fontsize=10)
    fig.tight_layout()
    save_fig(fig, OUT_PDF / "fig3_composite_vs_ine.pdf")
    plt.close(fig)
    print("[ok] fig3_composite_vs_ine.pdf")


# ---------- Fig 4 — sectoral decomposition ---------------------------

def fig4_sectoral_decomposition() -> None:
    # Satellite decomposition from the two-factor DFM loadings
    two = abs_path("data/satellite/dfm_twofactor_result.json")
    d = json.loads(two.read_text()) if two.exists() else {}
    fig, axes = plt.subplots(2, 1, figsize=(6.5, 6.0), sharex=True)

    if d.get("status") == "ok" and d.get("factors"):
        idx = pd.to_datetime(d["factor_index"])
        blocks = d.get("factors", {})
        weights = d.get("weights", {})
        # Contributions = weight * factor, quarterly aggregation
        frame = pd.DataFrame({k: v for k, v in blocks.items()}, index=idx)
        weighted = frame.multiply([weights.get(c, 0) for c in frame.columns], axis=1)
        q = weighted.resample("QE").mean()
        q = q[q.index.year >= 2023]

        colors = {"urban": PALETTE["positive"], "extractive": PALETTE["negative"],
                  "agricultural": PALETTE["tertiary"]}
        bottom = np.zeros(len(q))
        for col in q.columns:
            axes[0].bar(q.index, q[col], width=80, bottom=bottom,
                         color=colors.get(col, PALETTE["neutral"]),
                         label=col, edgecolor="white", linewidth=0.3)
            bottom = bottom + q[col].values
        axes[0].axhline(0, color="black", lw=0.4)
        axes[0].legend(loc="upper right", frameon=False, fontsize=8)
        axes[0].set_ylabel("weighted z-score")
        axes[0].set_title("Satellite-implied channel contributions (quarterly)",
                          loc="left", fontsize=9)
    else:
        _tbd_panel(axes[0], "two-factor DFM pending")

    # INE sectoral contributions on bottom — use parsed sectoral panel
    sect = abs_path("data/official/ine_gdp_quarterly_sectoral.csv")
    if sect.exists():
        ss = pd.read_csv(sect, parse_dates=["date"])
        ss = ss[ss["date"].dt.year >= 2023].sort_values("date")
        if not ss.empty:
            cols = {}
            for c in ss.columns:
                u = str(c).upper()
                if "PETRÓLEO" in u and "GAS" in u:
                    cols.setdefault("hydrocarbons", []).append(c)
                elif "AGRICULTURA" in u:
                    cols.setdefault("agriculture", []).append(c)
                elif "CONSTRUCCIÓN" in u or "CONSTRUCCION" in u:
                    cols.setdefault("construction", []).append(c)
                elif "COMERCIO" in u:
                    cols.setdefault("commerce", []).append(c)
                elif "INDUSTRIA" in u:
                    cols.setdefault("industry", []).append(c)
            # Plot YoY contribution: each sector's growth scaled to approximate
            # contribution. Simpler: plot level in each sector indexed.
            colors = {"hydrocarbons": PALETTE["negative"], "urban": PALETTE["positive"],
                      "agriculture": PALETTE["tertiary"], "industry": PALETTE["neutral"]}
            bottom = np.zeros(len(ss))
            plotted = 0
            for key, srcs in cols.items():
                if not srcs: continue
                vals = ss[srcs[0]].pct_change(4).fillna(0) * 100
                axes[1].bar(ss["date"], vals, width=80, bottom=bottom,
                             color=colors.get(key, PALETTE["neutral"]),
                             label=key, edgecolor="white", linewidth=0.3)
                bottom = bottom + vals.values
                plotted += 1
            if plotted:
                axes[1].axhline(0, color="black", lw=0.4)
                axes[1].legend(loc="upper right", frameon=False, fontsize=8)
                axes[1].set_ylabel("y/y %")
                axes[1].set_title("INE reported sectoral growth (y/y, %)",
                                  loc="left", fontsize=9)
            else:
                _tbd_panel(axes[1], "INE sectoral columns not matched")
        else:
            _tbd_panel(axes[1], "INE sectoral data not in 2023+ range")
    else:
        _tbd_panel(axes[1], "INE sectoral panel not loaded")

    fig.tight_layout()
    save_fig(fig, OUT_PDF / "fig4_sectoral_decomposition.pdf")
    plt.close(fig)
    print("[ok] fig4_sectoral_decomposition.pdf")


# ---------- Fig 5 — manipulation verdict ----------------------------

def fig5_manipulation_verdict() -> None:
    fig, (axL, axR) = plt.subplots(1, 2, figsize=(10.0, 4.0))

    # Left: scatter of VNF annual sum-RH vs YPFB annual production
    wbf = abs_path("data/official/wb_ggfr_bolivia_annual.csv")
    ypfb = abs_path("data/official/ypfb_gas_production_annual.csv")
    has_triang = wbf.exists() and ypfb.exists()
    if has_triang:
        wb = pd.read_csv(wbf)
        yp = pd.read_csv(ypfb)
        m = wb.merge(yp, on="year")
        if "flare_volume_bcm" in m.columns:
            m = m[(m["flare_volume_bcm"] > 0) & (m["gas_prod_mmm3d"] > 0)]
            pre = m[m["year"] < 2024]
            crisis = m[(m["year"] >= 2024) & (m["year"] <= 2025)]
            rest = m[m["year"] > 2025]
            # Pre-crisis OLS
            from numpy.polynomial import polynomial as P
            if len(pre) >= 3:
                xlog = np.log(pre["flare_volume_bcm"])
                ylog = np.log(pre["gas_prod_mmm3d"])
                slope, intercept = np.polyfit(xlog, ylog, 1)
                # R²
                resid = ylog - (intercept + slope * xlog)
                r2 = 1 - np.var(resid) / np.var(ylog)
                xs = np.linspace(xlog.min(), np.log(m["flare_volume_bcm"]).max(), 50)
                axL.plot(xs, intercept + slope * xs, color=PALETTE["neutral"],
                         lw=0.8, zorder=2)
                axL.text(0.05, 0.95, rf"$\hat\beta = {slope:.2f}$, $R^2 = {r2:.2f}$",
                         transform=axL.transAxes, va="top", fontsize=8)
            axL.scatter(np.log(pre["flare_volume_bcm"]), np.log(pre["gas_prod_mmm3d"]),
                         facecolors="none", edgecolors=PALETTE["neutral"],
                         s=30, label="pre-crisis (<2024)")
            if len(crisis):
                axL.scatter(np.log(crisis["flare_volume_bcm"]), np.log(crisis["gas_prod_mmm3d"]),
                             color=PALETTE["tertiary"], s=30, label="2024--2025")
            if len(rest):
                axL.scatter(np.log(rest["flare_volume_bcm"]), np.log(rest["gas_prod_mmm3d"]),
                             color=PALETTE["negative"], s=30, label="2026+")
            axL.set_xlabel(r"$\log(\Sigma\,\mathrm{BCM\ flared,\ WB GGFR})$")
            axL.set_ylabel(r"$\log(\mathrm{YPFB\ gas\ prod.})$")
            axL.legend(loc="lower right", frameon=False, fontsize=8)
            axL.set_title("Annual: WB-GGFR flaring vs YPFB production",
                          loc="left", fontsize=9)
    else:
        _tbd_panel(axL, "VNF monthly required")

    # Right: INE hydrocarbon VA vs YPFB-reported vs VNF-implied, indexed 2019=100
    hv = abs_path("data/official/ine_hydrocarbon_va.csv")
    if hv.exists() and ypfb.exists():
        h = pd.read_csv(hv, parse_dates=["date"])
        # Year-level reshape
        h["year"] = h["date"].dt.year
        h_ann = h.groupby("year", as_index=False)["hydrocarbon_va"].mean()
        merged = h_ann.merge(pd.read_csv(ypfb), on="year", how="inner")
        if wbf.exists():
            wb_ann = pd.read_csv(wbf)[["year", "flare_volume_bcm"]]
            merged = merged.merge(wb_ann, on="year", how="left")
        base = merged[merged["year"] == 2019]
        if not base.empty:
            b_hv = base["hydrocarbon_va"].iloc[0]
            b_yp = base["gas_prod_mmm3d"].iloc[0]
            b_wb = base["flare_volume_bcm"].iloc[0] if "flare_volume_bcm" in base.columns else None
            merged["hv_idx"] = merged["hydrocarbon_va"] / b_hv * 100
            merged["yp_idx"] = merged["gas_prod_mmm3d"] / b_yp * 100
            if b_wb is not None and b_wb > 0:
                merged["wb_idx"] = merged["flare_volume_bcm"] / b_wb * 100
            axR.plot(merged["year"], merged["hv_idx"], color=PALETTE["neutral"],
                     lw=LINE["primary"], label="INE hydrocarbon VA")
            axR.plot(merged["year"], merged["yp_idx"], color=PALETTE["negative"],
                     lw=LINE["primary"], label="YPFB gas production")
            if "wb_idx" in merged.columns:
                axR.plot(merged["year"], merged["wb_idx"], color=PALETTE["positive"],
                         lw=LINE["primary"], label="WB-GGFR flaring (VNF-derived)")
            axR.axhline(100, color="black", lw=0.3, ls=":")
            axR.set_ylabel("Index 2019 = 100")
            axR.set_xlabel("")
            axR.legend(loc="upper right", frameon=False, fontsize=8)
            axR.set_title("INE VA vs YPFB vs WB-GGFR (indexed to 2019)",
                          loc="left", fontsize=9)
    else:
        _tbd_panel(axR, "INE hydrocarbon VA + YPFB required")

    fig.tight_layout()
    save_fig(fig, OUT_PDF / "fig5_manipulation_verdict.pdf")
    plt.close(fig)
    print("[ok] fig5_manipulation_verdict.pdf")


# ---------- Appendix A1 — spatial coverage ---------------------------

def figA1_spatial_coverage() -> None:
    from _common import buffers, flares, ndvi_zones, rois
    fig, axes = plt.subplots(1, 3, figsize=(10.0, 4.5))

    # Left: urban buffers scatter over Bolivia outline
    axL, axC, axR = axes
    for ax in axes:
        ax.set_xlim(-72, -57.0)
        ax.set_ylim(-23.5, -9.5)
        ax.set_aspect("equal")
        ax.grid(True, alpha=0.15, linewidth=0.3)

    # Urban buffers
    for city in buffers():
        circle = plt.Circle((city["lon"], city["lat"]),
                             city["radius_km"] / 111, fill=False,
                             color=PALETTE["positive"], lw=0.8)
        axL.add_patch(circle)
        axL.plot(city["lon"], city["lat"], "o", color=PALETTE["positive"],
                 markersize=3)
    axL.set_title("VIIRS DNB: 11 urban buffers", loc="left", fontsize=9)

    # Chaco flares
    bbox = flares()["bbox"]
    axC.add_patch(mpatches.Rectangle((bbox[0], bbox[1]),
                                       bbox[2] - bbox[0], bbox[3] - bbox[1],
                                       fill=False, color=PALETTE["negative"],
                                       lw=0.8))
    for fld in flares()["fields"]:
        axC.plot(fld["lon"], fld["lat"], "*",
                 color=PALETTE["negative"], markersize=8)
        axC.annotate(fld["name"][:4], (fld["lon"], fld["lat"]),
                     textcoords="offset points", xytext=(4, 2),
                     fontsize=6, color=PALETTE["neutral"])
    axC.set_title("VNF: Chaco basin + 7 fields", loc="left", fontsize=9)

    # TROPOMI ROIs + NDVI zones
    for roi in rois():
        axR.add_patch(mpatches.Rectangle(
            (min(roi["nw_lon"], roi["se_lon"]),
             min(roi["nw_lat"], roi["se_lat"])),
            abs(roi["se_lon"] - roi["nw_lon"]),
            abs(roi["nw_lat"] - roi["se_lat"]),
            fill=False, color=PALETTE["tertiary"], lw=0.8))
    for zone in ndvi_zones():
        circle = plt.Circle((zone["lon"], zone["lat"]),
                             zone["radius_km"] / 111,
                             fill=True, color=PALETTE["positive"],
                             alpha=0.15, lw=0.5)
        axR.add_patch(circle)
        axR.plot(zone["lon"], zone["lat"], "s",
                 color=PALETTE["positive"], markersize=3)
    axR.set_title(r"NO$_2$ ROIs + NDVI cropland zones", loc="left", fontsize=9)

    for ax in axes:
        ax.set_xlabel("longitude", fontsize=7)
        ax.set_ylabel("latitude", fontsize=7)

    fig.tight_layout()
    save_fig(fig, OUT_PDF / "figA1_spatial_coverage.pdf")
    plt.close(fig)
    print("[ok] figA1_spatial_coverage.pdf")


# ---------- Appendix A2 — field-level calibration --------------------

def figA2_field_calibration() -> None:
    fields = ["margarita", "huacaya", "san_alberto", "sabalo",
              "incahuasi", "aquio"]
    fig, axes = plt.subplots(2, 3, figsize=(10.0, 6.5))
    fc = abs_path("data/satellite/vnf_calibration_field.json")
    if not fc.exists() or json.loads(fc.read_text()).get("status") != "ok":
        for ax, name in zip(axes.flat, fields):
            _tbd_panel(ax, "YPFB field-month required")
            ax.set_title(name, loc="left", fontsize=9)
        fig.tight_layout()
        save_fig(fig, OUT_PDF / "figA2_field_calibration.pdf")
        plt.close(fig)
        print("[ok] figA2_field_calibration.pdf (TBD panels)")
        return
    # Real branch not yet exercised (field-month YPFB not in repo)
    d = json.loads(fc.read_text())
    for ax, name in zip(axes.flat, fields):
        r = d.get("per_field_first_diff", {}).get(name, {})
        if r.get("status") == "ok":
            beta, se, n, r2 = r["beta"], r["se"], r["n"], r["r2"]
            ax.text(0.5, 0.5,
                    rf"$\hat\beta = {beta:.2f}$\n(se {se:.2f})\n$n = {n}$, $R^2 = {r2:.2f}$",
                    transform=ax.transAxes, ha="center", va="center",
                    fontsize=9)
        else:
            _tbd_panel(ax, r.get("status", "unknown"))
        ax.set_title(name, loc="left", fontsize=9)
    fig.tight_layout()
    save_fig(fig, OUT_PDF / "figA2_field_calibration.pdf")
    plt.close(fig)
    print("[ok] figA2_field_calibration.pdf")


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
