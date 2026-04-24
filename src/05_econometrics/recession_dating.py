"""Recession dating — paper §4.2.

Two methods on the DFM factor $\\hat f_t$:

  * BBQ (Bry-Boschan quarterly, Harding-Pagan 2002) — deterministic
    peak/trough identification with phase and cycle constraints.
  * Hamilton (1989) two-state Markov-switching AR — monthly regime
    probabilities. Uses statsmodels' MarkovAutoregression.

When the DFM result is absent, we fall back to the satellite CI directly.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from _common import abs_path, load_env, paths  # noqa: E402

load_env()


def load_factor(raw: bool = False) -> pd.Series:
    """Return the DFM factor (or CI fallback). raw=True returns the un-z-scored
    factor which preserves cyclical amplitude — needed for Markov-switching
    regime separation."""
    p = paths()
    dfm_path = abs_path("data/satellite/dfm_result.json")
    if dfm_path.exists():
        d = json.loads(dfm_path.read_text())
        if d.get("status") == "ok":
            idx = pd.to_datetime(d["factor_index"])
            key = "factor" if (raw and "factor" in d) else "factor_z"
            return pd.Series(d[key], index=idx, name=key)
    ci = pd.read_csv(abs_path(p["data"]["ci"]), parse_dates=["date"])
    ci = ci.dropna(subset=["ci"]).sort_values("date")
    return ci.set_index("date")["ci"].rename("factor_z")


def bbq_monthly(y: pd.Series, min_phase: int = 6, min_cycle: int = 15) -> dict:
    """Bry-Boschan monthly algorithm (phase >= 6 months, cycle >= 15).

    Conservative: we identify candidate turning points as local extrema
    within a rolling 6-month window on each side, then enforce phase and
    cycle constraints. Alternating peaks and troughs are retained.
    """
    y = y.dropna()
    if len(y) < min_cycle * 2:
        return {"status": "insufficient_n", "n": int(len(y))}

    window = min_phase
    peaks, troughs = [], []
    for i in range(window, len(y) - window):
        w = y.iloc[i - window:i + window + 1]
        if y.iloc[i] == w.max() and w.idxmax() == y.index[i]:
            peaks.append(y.index[i])
        if y.iloc[i] == w.min() and w.idxmin() == y.index[i]:
            troughs.append(y.index[i])

    events = sorted([(t, "peak") for t in peaks] + [(t, "trough") for t in troughs])
    cleaned: list[tuple] = []
    for ev in events:
        if not cleaned:
            cleaned.append(ev)
            continue
        last = cleaned[-1]
        if ev[1] == last[1]:
            # two same-type in a row; keep the more extreme
            if ev[1] == "peak" and y.loc[ev[0]] > y.loc[last[0]]:
                cleaned[-1] = ev
            elif ev[1] == "trough" and y.loc[ev[0]] < y.loc[last[0]]:
                cleaned[-1] = ev
        else:
            # enforce min cycle from last same-type
            cleaned.append(ev)

    # Enforce min_cycle by dropping tight turning points
    final: list[tuple] = []
    for ev in cleaned:
        if len(final) < 2:
            final.append(ev)
            continue
        if (ev[0] - final[-2][0]).days / 30.4 < min_cycle:
            # drop the middle of the three
            final.pop()
        final.append(ev)

    return {
        "status": "ok",
        "n_obs": int(len(y)),
        "peaks": [t.strftime("%Y-%m") for t, k in final if k == "peak"],
        "troughs": [t.strftime("%Y-%m") for t, k in final if k == "trough"],
        "alternating": [(t.strftime("%Y-%m"), k) for t, k in final],
    }


def hamilton_switching(y: pd.Series) -> dict:
    """Two-state Markov-switching on the factor's monthly CHANGE. Regime 1 =
    recession (negative mean), regime 0 = expansion (positive mean). The
    factor level is highly persistent (autocorr ≈ 1), so switching on the
    level alone fails to separate regimes; the first difference is what
    the Hamilton framework is designed for. Returns smoothed recession
    probabilities per month."""
    from statsmodels.tsa.regime_switching.markov_regression import MarkovRegression
    y = y.dropna()
    if len(y) < 60:
        return {"status": "insufficient_n", "n": int(len(y))}
    dy = y.diff().dropna()
    try:
        mod = MarkovRegression(dy, k_regimes=2, trend="c", switching_variance=True)
        res = mod.fit(disp=False)
        mu0 = float(res.params["const[0]"])
        mu1 = float(res.params["const[1]"])
        rec_regime = 1 if mu1 < mu0 else 0
        p_rec = res.smoothed_marginal_probabilities[rec_regime]
        return {
            "status": "ok",
            "n_obs": int(len(dy)),
            "recession_mean": min(mu0, mu1),
            "expansion_mean": max(mu0, mu1),
            "index": [d.strftime("%Y-%m") for d in p_rec.index],
            "p_recession": p_rec.round(3).tolist(),
            "log_likelihood": float(res.llf),
            "model": "MarkovRegression on first-differenced factor",
        }
    except Exception as e:
        return {"status": "fit_failed", "error": str(e)[:200]}


def main() -> None:
    y_z = load_factor(raw=False)      # z-scored: clean BBQ turning points
    y_raw = load_factor(raw=True)     # raw amplitude: Markov regime separation
    out = {
        "bbq": bbq_monthly(y_z),
        "markov_switching": hamilton_switching(y_raw),
    }
    out_path = abs_path("data/satellite/recession_dating.json")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(out, indent=2))
    bbq = out["bbq"]
    if bbq.get("status") == "ok":
        print(f"[ok] BBQ: {len(bbq['peaks'])} peaks, {len(bbq['troughs'])} troughs")
    ms = out["markov_switching"]
    if ms.get("status") == "ok":
        rec = [(d, p) for d, p in zip(ms["index"], ms["p_recession"]) if p > 0.5]
        print(f"[ok] Markov-switching: {len(rec)} months with P(rec)>0.5")


if __name__ == "__main__":
    main()
