"""Paper-specific figures under paper/figures/ (separate from outputs/figures/
which serves the weekly brief and monthly report).

Three figures for the results section:
  * factor_and_bbq.pdf     — DFM factor with BBQ peak/trough markers
  * markov_probability.pdf — Hamilton recession probabilities
  * ine_vs_satellite.pdf   — quarterly INE GDP growth overlaid on satellite factor
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import matplotlib
import numpy as np
import pandas as pd

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from _common import abs_path, load_env, paths  # noqa: E402

load_env()


def _pagella() -> None:
    for fam in ("TeX Gyre Pagella", "Palatino", "Palatino Linotype", "STIX"):
        try:
            plt.rcParams["font.family"] = "serif"
            plt.rcParams["font.serif"] = [fam]
            return
        except Exception:
            continue


def load_factor_df() -> pd.DataFrame:
    p = paths()
    dfm = abs_path("data/satellite/dfm_result.json")
    if dfm.exists() and (d := json.loads(dfm.read_text())).get("status") == "ok":
        return pd.DataFrame({
            "date": pd.to_datetime(d["factor_index"]),
            "factor": d["factor_z"],
        })
    ci = pd.read_csv(abs_path(p["data"]["ci"]), parse_dates=["date"])
    return ci.rename(columns={"ci": "factor"})[["date", "factor"]].dropna()


def fig_factor_bbq(palette: dict, out_path: Path) -> None:
    df = load_factor_df().sort_values("date")
    dat = abs_path("data/satellite/recession_dating.json")
    bbq = json.loads(dat.read_text())["bbq"] if dat.exists() else {"status": "none"}

    fig, ax = plt.subplots(figsize=(9, 3.8))
    if df.empty:
        ax.text(0.5, 0.5, "Satellite factor not yet computed",
                ha="center", va="center", transform=ax.transAxes)
        fig.tight_layout(); fig.savefig(out_path); plt.close(fig); return
    ax.plot(df["date"], df["factor"], color=palette["neutral"], lw=1.0)
    pos_mask = df["factor"].fillna(0) >= 0
    ax.fill_between(df["date"], df["factor"].fillna(0), 0,
                    where=pos_mask, color=palette["positive"], alpha=0.15)
    ax.fill_between(df["date"], df["factor"].fillna(0), 0,
                    where=~pos_mask, color=palette["negative"], alpha=0.15)
    if bbq.get("status") == "ok":
        for p in bbq["peaks"]:
            ax.axvline(pd.Timestamp(p + "-15"), color=palette["negative"], ls=":", lw=0.8)
        for t in bbq["troughs"]:
            ax.axvline(pd.Timestamp(t + "-15"), color=palette["positive"], ls=":", lw=0.8)
    ax.axhline(0, color="black", lw=0.4)
    ax.set_title("Bolivia monthly satellite factor with BBQ turning points")
    ax.set_ylabel("z-score")
    fig.tight_layout()
    fig.savefig(out_path)
    plt.close(fig)


def fig_markov(palette: dict, out_path: Path) -> None:
    dat = abs_path("data/satellite/recession_dating.json")
    if not dat.exists():
        return
    d = json.loads(dat.read_text()).get("markov_switching", {})
    if d.get("status") != "ok":
        fig, ax = plt.subplots(figsize=(8, 2.5))
        ax.text(0.5, 0.5, f"Markov-switching unavailable ({d.get('status','n/a')})",
                ha="center", va="center", transform=ax.transAxes)
        fig.tight_layout(); fig.savefig(out_path); plt.close(fig); return
    idx = pd.to_datetime(d["index"])
    p = np.asarray(d["p_recession"])
    fig, ax = plt.subplots(figsize=(8, 2.8))
    ax.fill_between(idx, p, color=palette["negative"], alpha=0.5, step="post")
    ax.axhline(0.5, color="black", lw=0.3, ls="--")
    ax.set_ylim(0, 1)
    ax.set_title("Hamilton two-state Markov-switching recession probability")
    ax.set_ylabel(r"$P(\mathrm{recession}|\text{data}_t)$")
    fig.tight_layout()
    fig.savefig(out_path)
    plt.close(fig)


def fig_ine_vs_factor(palette: dict, out_path: Path) -> None:
    df = load_factor_df().sort_values("date").set_index("date")
    ine_path = abs_path("data/official/ine_gdp_quarterly.csv")
    fig, ax = plt.subplots(figsize=(9, 3.8))
    ax.plot(df.index, df["factor"], color=palette["neutral"], lw=1.0,
            label="Satellite factor (z)")
    if ine_path.exists():
        ine = pd.read_csv(ine_path, parse_dates=["date"]).set_index("date")
        g = np.log(ine["gdp_real"]).diff(4)
        # Standardize quarterly growth for visual comparability
        gz = (g - g.mean()) / g.std(ddof=1)
        ax.plot(gz.index, gz.values, color=palette["negative"], lw=1.2,
                marker="o", ms=3, label="INE GDP growth YoY (z)")
    else:
        ax.text(0.02, 0.92, "INE GDP series not loaded",
                transform=ax.transAxes, color="grey")
    ax.axhline(0, color="black", lw=0.4)
    ax.legend(loc="best", frameon=False)
    ax.set_title("Satellite factor vs INE official GDP growth")
    fig.tight_layout()
    fig.savefig(out_path)
    plt.close(fig)


def main() -> None:
    _pagella()
    p = paths()
    out_dir = abs_path("paper/figures")
    out_dir.mkdir(parents=True, exist_ok=True)
    palette = p["palette"]
    fig_factor_bbq(palette, out_dir / "factor_and_bbq.pdf")
    fig_markov(palette, out_dir / "markov_probability.pdf")
    fig_ine_vs_factor(palette, out_dir / "ine_vs_satellite.pdf")
    print(f"[ok] paper figures → {out_dir}/")


if __name__ == "__main__":
    main()
