"""INE IGAE-disagreement alert (spec §Failure modes).

If the satellite CI deviates > 1.5σ from the INE IGAE signal in a given
month, log the incident, flag for La Linterna desk review, and block
auto-publication. Also fires on two-consecutive-months sign flip of any
benchmark β coefficient (from benchmark_ine.json).

The "IGAE signal" is the z-score of 12-month-log-growth in IGAE against
its 2015–2019 distribution; the satellite CI is already z-scored. We
compare them on the same axis.

Emits data/satellite/igae_disagreement.json with:
  latest_month, ci_z, igae_z, gap, beta_sign_flip, halt_publication
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


BASELINE_START = "2015-01-01"
BASELINE_END = "2019-12-31"
DEVIATION_THRESHOLD = 1.5


def igae_signal(igae_path: Path) -> pd.DataFrame:
    if not igae_path.exists():
        return pd.DataFrame(columns=["date", "igae_z"])
    df = pd.read_csv(igae_path, parse_dates=["date"])
    if "igae" not in df.columns:
        return pd.DataFrame(columns=["date", "igae_z"])
    df = df.sort_values("date").reset_index(drop=True)
    df["log_growth"] = np.log(df["igae"]) - np.log(df["igae"].shift(12))
    base = df[(df["date"] >= BASELINE_START) & (df["date"] <= BASELINE_END)]
    mu, sd = base["log_growth"].mean(), base["log_growth"].std(ddof=1)
    df["igae_z"] = (df["log_growth"] - mu) / (sd if sd and sd > 0 else 1.0)
    return df[["date", "igae_z"]]


def beta_sign_flip(bench_path: Path, history_path: Path) -> bool:
    """Append current betas to the history file; return True if any
    coefficient has flipped sign on 2 consecutive runs."""
    if not bench_path.exists():
        return False
    b = json.loads(bench_path.read_text())
    if b.get("status") != "ok":
        return False
    row = {"run_ts": pd.Timestamp.utcnow().isoformat(), **b["betas"]}
    if history_path.exists():
        hist = pd.read_csv(history_path)
        hist = pd.concat([hist, pd.DataFrame([row])], ignore_index=True)
    else:
        hist = pd.DataFrame([row])
    hist.to_csv(history_path, index=False)

    if len(hist) < 3:
        return False
    last3 = hist.tail(3)
    for k in ("viirs_z", "vnf_z", "no2_z"):
        signs = np.sign(last3[k].to_numpy())
        # sign flip on the latest run AND the previous run both differ from pre-previous
        if signs[-1] != signs[-3] and signs[-2] != signs[-3] and signs[-1] == signs[-2]:
            return True
    return False


def main() -> None:
    p = paths()
    ci = pd.read_csv(abs_path(p["data"]["ci"]), parse_dates=["date"])
    ci = ci.dropna(subset=["ci"]).sort_values("date")
    igae = igae_signal(abs_path(p["data"]["official_igae"]))

    merged = ci.merge(igae, on="date", how="left").sort_values("date")
    merged["gap"] = (merged["ci"] - merged["igae_z"]).abs()
    merged["alert"] = merged["gap"] > DEVIATION_THRESHOLD

    latest = merged.iloc[-1].to_dict() if not merged.empty else {}

    bench_path = abs_path("data/satellite/benchmark_ine.json")
    history_path = abs_path("data/satellite/benchmark_history.csv")
    history_path.parent.mkdir(parents=True, exist_ok=True)
    flip = beta_sign_flip(bench_path, history_path)

    gap = float(latest.get("gap", np.nan)) if latest else np.nan
    alert_fire = bool(latest.get("alert", False)) or flip

    out = {
        "latest_month": str(latest.get("date", "")) if latest else None,
        "ci": float(latest.get("ci", np.nan)) if latest else None,
        "igae_z": float(latest.get("igae_z", np.nan)) if latest and pd.notna(latest.get("igae_z")) else None,
        "gap": None if pd.isna(gap) else gap,
        "deviation_threshold_sigma": DEVIATION_THRESHOLD,
        "beta_sign_flip": bool(flip),
        "alert": alert_fire,
        "halt_publication": alert_fire,
        "action": ("Log incident; route to La Linterna desk review within 48h; "
                   "do not auto-publish." if alert_fire else "No action."),
    }

    out_path = abs_path("data/satellite/igae_disagreement.json")
    out_path.write_text(json.dumps(out, indent=2))
    print(f"[ok] igae_disagreement → {out_path}  alert={alert_fire}")


if __name__ == "__main__":
    main()
