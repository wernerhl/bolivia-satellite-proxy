"""La Linterna publication style for matplotlib (Track H standing conventions).

Usage:
    from lalinterna_style import apply_style, PALETTE, save_fig
    apply_style()
    fig, ax = plt.subplots(figsize=(6.0, 3.5))
    ax.plot(x, y, color=PALETTE["positive"], lw=1.2)
    save_fig(fig, "figures/pdf/fig1.pdf")
"""
from __future__ import annotations

import os
from pathlib import Path

import matplotlib
import matplotlib.pyplot as plt


PALETTE = {
    "positive": "#1F6F73",   # teal — positive anomaly / expansion
    "negative": "#A13D2D",   # rust — negative anomaly / contraction
    "neutral":  "#3B4A54",   # slate — neutral / reference
    "tertiary": "#C08A3E",   # ochre — tertiary accent
}


LINE = {"primary": 1.2, "comparison": 0.8, "spine": 0.5, "grid": 0.3}


def apply_style(use_tex: bool | None = None) -> bool:
    """Apply standing style. Returns True if LaTeX rendering is enabled."""
    matplotlib.use("Agg", force=True)
    # Probe LaTeX availability
    if use_tex is None:
        from shutil import which
        use_tex = bool(which("latex") or which("pdflatex"))

    for fam in ("TeX Gyre Pagella", "Palatino", "Palatino Linotype", "STIX"):
        try:
            plt.rcParams["font.family"] = "serif"
            plt.rcParams["font.serif"] = [fam]
            break
        except Exception:
            continue

    plt.rcParams["axes.linewidth"] = LINE["spine"]
    plt.rcParams["xtick.major.width"] = LINE["spine"]
    plt.rcParams["ytick.major.width"] = LINE["spine"]
    plt.rcParams["axes.grid"] = False
    plt.rcParams["axes.spines.top"] = False
    plt.rcParams["axes.spines.right"] = False
    plt.rcParams["legend.frameon"] = False
    plt.rcParams["legend.fontsize"] = 8
    plt.rcParams["axes.labelsize"] = 9
    plt.rcParams["axes.titlesize"] = 10
    plt.rcParams["xtick.labelsize"] = 8
    plt.rcParams["ytick.labelsize"] = 8
    plt.rcParams["pdf.fonttype"] = 42
    plt.rcParams["ps.fonttype"] = 42
    if use_tex:
        try:
            plt.rcParams["text.usetex"] = True
            plt.rcParams["text.latex.preamble"] = r"\usepackage{mathpazo}"
        except Exception:
            plt.rcParams["text.usetex"] = False
            use_tex = False
    return use_tex


def save_fig(fig, pdf_path: str | Path, png_root: str | Path = "figures/png") -> None:
    """Save PDF at 400 dpi and a PNG mirror at 150 dpi for dashboard use."""
    pdf_path = Path(pdf_path)
    pdf_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(pdf_path, dpi=400, bbox_inches="tight", pad_inches=0.05)
    png = Path(png_root) / (pdf_path.stem + ".png")
    png.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(png, dpi=150, bbox_inches="tight", pad_inches=0.05)


def reference_line(ax, date, label: str, color: str | None = None) -> None:
    """Vertical dashed reference with a 90°-rotated inline label at 7pt."""
    color = color or PALETTE["neutral"]
    ax.axvline(date, color=color, linestyle="--", linewidth=0.6, alpha=0.7)
    ax.text(date, ax.get_ylim()[1] * 0.98, label, rotation=90,
            ha="right", va="top", fontsize=7, color=color, alpha=0.9)


def recession_band(ax, start, end) -> None:
    """Shaded axvspan at alpha=0.08, neutral color."""
    ax.axvspan(start, end, color=PALETTE["neutral"], alpha=0.08, linewidth=0)
