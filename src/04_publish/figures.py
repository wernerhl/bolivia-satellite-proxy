"""Render the three PDF figures for the weekly/monthly publication.

Uses TeX Gyre Pagella when available (falls back to Palatino, then STIX);
the La Linterna palette is fixed in config/paths.yaml.

Figures:
  satellite_ci.pdf          — CI time series with ±1σ, ±2σ bands
  vnf_chaco_vs_ypfb.pdf     — calibration scatter with residual band
  no2_metros.pdf            — La Paz + Santa Cruz z-score series
"""
from __future__ import annotations

import sys
from pathlib import Path

import matplotlib
import numpy as np
import pandas as pd

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from _common import abs_path, ensure_dir, load_env, paths  # noqa: E402

load_env()


def _set_pagella() -> None:
    for fam in ("TeX Gyre Pagella", "Palatino", "Palatino Linotype", "STIX"):
        try:
            plt.rcParams["font.family"] = "serif"
            plt.rcParams["font.serif"] = [fam]
            return
        except Exception:
            continue


def fig_ci(ci: pd.DataFrame, out_path: Path, palette: dict) -> None:
    ci = ci.copy()
    ci["date"] = pd.to_datetime(ci["date"])
    ci = ci.dropna(subset=["ci"]).sort_values("date")

    fig, ax = plt.subplots(figsize=(8, 4))
    ax.axhspan(-2, 2, color=palette["neutral"], alpha=0.06)
    ax.axhspan(-1, 1, color=palette["neutral"], alpha=0.10)
    pos = ci["ci"] >= 0
    ax.plot(ci["date"], ci["ci"], color=palette["neutral"], lw=1.0)
    ax.scatter(ci.loc[pos, "date"], ci.loc[pos, "ci"],
               color=palette["positive"], s=10, zorder=3)
    ax.scatter(ci.loc[~pos, "date"], ci.loc[~pos, "ci"],
               color=palette["negative"], s=10, zorder=3)
    ax.axhline(0, color="black", lw=0.4)
    ax.set_title("Bolivia satellite-only coincident index")
    ax.set_ylabel("z-score")
    ax.set_xlabel("")
    fig.tight_layout()
    fig.savefig(out_path)
    plt.close(fig)


def fig_vnf_cal(vnf: pd.DataFrame, out_path: Path, palette: dict) -> None:
    fig, ax = plt.subplots(figsize=(6, 5))
    if "residual" in vnf.columns and vnf["residual"].notna().any():
        r = vnf.dropna(subset=["log_rh", "residual"])
        ax.scatter(r["log_rh"], r["residual"], s=12,
                   color=palette["neutral"], alpha=0.7)
        ax.axhline(0, color="black", lw=0.4)
        ax.axhline(r["residual"].std(ddof=1) * 2, ls=":", color=palette["negative"])
        ax.axhline(-r["residual"].std(ddof=1) * 2, ls=":", color=palette["negative"])
        ax.set_xlabel(r"$\log(\Sigma RH_\mathrm{Chaco})$")
        ax.set_ylabel(r"residual $\log(YPFB)$ vs $\log(\Sigma RH)$")
        ax.set_title("VNF Chaco — calibration vs YPFB gas output")
    else:
        ax.text(0.5, 0.5, "YPFB official series not loaded",
                ha="center", va="center", transform=ax.transAxes)
    fig.tight_layout()
    fig.savefig(out_path)
    plt.close(fig)


def fig_no2(no2: pd.DataFrame, out_path: Path, palette: dict) -> None:
    no2 = no2.copy()
    no2["date"] = pd.to_datetime(no2["date"])
    fig, ax = plt.subplots(figsize=(8, 4))
    colors = {"la_paz_el_alto": palette["positive"], "santa_cruz": palette["negative"]}
    for name, color in colors.items():
        g = no2[no2["roi"] == name].sort_values("date")
        if g.empty:
            continue
        ax.plot(g["date"], g["z_vs_2019"], color=color, lw=1.2,
                label=name.replace("_", " "))
    ax.axhline(0, color="black", lw=0.4)
    ax.legend(loc="best", frameon=False)
    ax.set_title("Sentinel-5P NO₂ z-score vs 2019 baseline")
    ax.set_ylabel("z-score")
    fig.tight_layout()
    fig.savefig(out_path)
    plt.close(fig)


def main() -> None:
    _set_pagella()
    p = paths()
    palette = p["palette"]
    ensure_dir(abs_path(p["outputs"]["figures"]))

    try:
        ci = pd.read_csv(abs_path(p["data"]["ci"]))
        fig_ci(ci, abs_path(p["outputs"]["fig_ci"]), palette)
        print("[ok] satellite_ci.pdf")
    except Exception as e:
        print(f"[warn] CI figure skipped: {e}")

    try:
        vnf = pd.read_csv(abs_path(p["data"]["vnf_anomaly"]))
        fig_vnf_cal(vnf, abs_path(p["outputs"]["fig_vnf_cal"]), palette)
        print("[ok] vnf_chaco_vs_ypfb.pdf")
    except Exception as e:
        print(f"[warn] VNF calibration figure skipped: {e}")

    try:
        no2 = pd.read_csv(abs_path(p["data"]["s5p_anomaly"]))
        fig_no2(no2, abs_path(p["outputs"]["fig_no2"]), palette)
        print("[ok] no2_metros.pdf")
    except Exception as e:
        print(f"[warn] NO₂ figure skipped: {e}")


if __name__ == "__main__":
    main()
