"""Track F — dashboard vintage stamp.

Writes outputs/dashboard/vintage.json with the current UTC timestamp, the
git commit hash, the latest data-date available in each satellite stream,
and a "stable vintage" date equal to today minus 60 days (per the release
cadence in the agent prompt). Downstream Jekyll / static-site generators
can ingest this JSON to render a dateline that never silently overwrites
prior readings.
"""
from __future__ import annotations

import json
import subprocess
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from _common import abs_path, ensure_dir, load_env, paths  # noqa: E402

load_env()


def _git_commit() -> str | None:
    try:
        return subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    except Exception:
        return None


def _latest(path: Path, date_col: str = "date") -> str | None:
    if not path.exists():
        return None
    try:
        df = pd.read_csv(path, parse_dates=[date_col])
        if df.empty or df[date_col].isna().all():
            return None
        return df[date_col].max().strftime("%Y-%m-%d")
    except Exception:
        return None


def main() -> None:
    p = paths()
    now = datetime.now(timezone.utc)
    stable_vintage = (date.today() - timedelta(days=60)).isoformat()

    vintage = {
        "generated_at": now.isoformat(timespec="seconds"),
        "git_commit": _git_commit(),
        "stable_vintage_date": stable_vintage,
        "stream_latest": {
            "viirs_sol_monthly": _latest(abs_path(p["data"]["viirs_sol_monthly"])),
            "vnf_monthly": _latest(abs_path(p["data"]["vnf_monthly"])),
            "s5p_monthly": _latest(abs_path(p["data"]["s5p_monthly"])),
            "s2_ndvi_monthly": _latest(abs_path(p["data"]["s2_ndvi_monthly"])),
            "ci": _latest(abs_path(p["data"]["ci"])),
        },
        "cadence_notes": (
            "Satellite streams update 48-72h after month end. INE and YPFB "
            "publish 45-75 days after month end. Full composite reaches a "
            "stable vintage 60 days after the month it describes. Historical "
            "vintages are preserved under outputs/dashboard/vintage_archive/ "
            "so researchers can reconstruct what was known when."
        ),
    }

    out_dir = ensure_dir(abs_path("outputs/dashboard"))
    stamp = out_dir / "vintage.json"
    stamp.write_text(json.dumps(vintage, indent=2))

    # Archive a dated copy (never overwrite)
    archive_dir = ensure_dir(out_dir / "vintage_archive")
    dated = archive_dir / f"vintage_{now.strftime('%Y%m%dT%H%M%SZ')}.json"
    dated.write_text(json.dumps(vintage, indent=2))

    print(f"[ok] vintage stamp → {stamp}")
    print(f"[ok] archived copy → {dated}")


if __name__ == "__main__":
    main()
