"""Render the Monday 06:00 America/La_Paz weekly brief.

Output:
  outputs/satellite_bolivia_brief.md   — plaintext summary (streams + CI)
  outputs/social_post.txt              — 240-char Bluesky/X draft

The brief reports the latest publishable month (current − 2 for VIIRS;
most recent month satisfying ≥15 valid S5P days; most recent VNF month).
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from _common import abs_path, load_env, paths  # noqa: E402

load_env()


def latest(df: pd.DataFrame, col: str | None = None) -> pd.Series | None:
    if df.empty:
        return None
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date")
    if col is not None:
        df = df.dropna(subset=[col])
    if df.empty:
        return None
    return df.iloc[-1]


def sign_word(x: float) -> str:
    if pd.isna(x):
        return "neutral"
    if x > 0.5:
        return "expanding"
    if x < -0.5:
        return "contracting"
    return "flat"


def main() -> None:
    p = paths()
    ci = pd.read_csv(abs_path(p["data"]["ci"]))
    viirs = pd.read_csv(abs_path(p["data"]["viirs_sol_anomaly"]))
    vnf = pd.read_csv(abs_path(p["data"]["vnf_anomaly"]))
    no2 = pd.read_csv(abs_path(p["data"]["s5p_anomaly"]))

    row = latest(ci, col="ci")
    month = row["date"].strftime("%Y-%m") if row is not None else "n/a"
    ci_val = float(row["ci"]) if row is not None else float("nan")
    v_z = float(row["viirs_z"]) if row is not None and pd.notna(row["viirs_z"]) else float("nan")
    f_z = float(row["vnf_z"]) if row is not None and pd.notna(row["vnf_z"]) else float("nan")
    n_z = float(row["no2_z"]) if row is not None and pd.notna(row["no2_z"]) else float("nan")

    vnf_flag = bool(vnf["flag_manip"].iloc[-1]) if not vnf.empty and "flag_manip" in vnf.columns else False
    volcanic = (
        no2.get("volcanic_flag", pd.Series(dtype=bool)).fillna(False).tail(3).any()
        if not no2.empty else False
    )

    md_path = abs_path(p["outputs"]["brief"])
    md_path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        f"# Bolivia satellite-proxy brief — {month}",
        "",
        f"- **CI:** {ci_val:+.2f} ({sign_word(ci_val)})",
        f"- VIIRS SOL composite z: {v_z:+.2f}",
        f"- VNF Chaco z: {f_z:+.2f}  " + ("(MANIPULATION FLAG)" if vnf_flag else ""),
        f"- S5P NO₂ composite z: {n_z:+.2f}  " + ("(volcanic flag in past 3m)" if volcanic else ""),
        "",
        "## Methodology",
        "- Weights: 0.40 VIIRS, 0.30 VNF, 0.30 NO₂. Baselines and masking per agent spec.",
        "- All three streams publish at monthly frequency; this brief is internal weekly.",
        "",
        "## Disclaimer",
        "Opinions do not reflect affiliated institutions; all errors are the author's.",
        "Contact: wernerhl@gmail.com.",
    ]
    md_path.write_text("\n".join(lines))
    print(f"[ok] wrote {md_path}")

    # Social post — 240 chars
    post = (
        f"Bolivia satellite CI {month}: {ci_val:+.2f} ({sign_word(ci_val)}). "
        f"VIIRS {v_z:+.2f}  VNF {f_z:+.2f}  NO₂ {n_z:+.2f}. "
        "Brief → lalinterna."
    )[:240]
    sp_path = abs_path(p["outputs"]["social_post"])
    sp_path.write_text(post + "\n")
    print(f"[ok] wrote {sp_path}")


if __name__ == "__main__":
    main()
