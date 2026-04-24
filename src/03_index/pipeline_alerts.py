"""Pipeline-health alerts (spec §Failure modes, rows 6–8).

  * Zero-variance halt — any stream with sd == 0 on trailing 2 months → halt.
  * VNF Chaco RH MoM drop > 30% with no YPFB announcement → alert. YPFB
    announcement detection is out of scope; we surface the trigger for the
    La Linterna desk to investigate within 48h.
  * VIIRS city SOL YoY drop > 15% for a major city (La Paz–El Alto,
    Santa Cruz, Cochabamba) outside the documented cloud-cover window for
    that city → alert; cross-check electricity demand is a desk task.

Writes data/satellite/pipeline_alerts.json.
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


MAJOR_CITIES = ("la_paz_el_alto", "santa_cruz", "cochabamba")
# Known cloud-cover months where a YoY drop is not evidence of anything real.
CLOUD_WINDOWS = {
    "la_paz_el_alto": {1, 2, 3},        # Jan–Mar (spec §S5P pathologies, transfers)
    "santa_cruz": {11, 12, 1, 2, 3},    # Nov–Mar (wet season)
    "cochabamba": set(),
}


def _read_or_empty(path, **kw) -> pd.DataFrame:
    return pd.read_csv(path, **kw) if path.exists() else pd.DataFrame()


def zero_variance_check() -> list[dict]:
    p = paths()
    events: list[dict] = []

    viirs = _read_or_empty(abs_path(p["data"]["viirs_sol_monthly"]))
    if not viirs.empty:
        viirs["date"] = pd.to_datetime(viirs["date"])
        for city, g in viirs.groupby("city"):
            last2 = g.sort_values("date").tail(2)["sol"]
            if len(last2) == 2 and last2.std(ddof=0) == 0:
                events.append({"stream": "viirs", "unit": city,
                               "condition": "zero_variance_trailing_2m"})

    vnf = _read_or_empty(abs_path(p["data"]["vnf_monthly"]))
    if not vnf.empty:
        vnf["date"] = pd.to_datetime(vnf["date"])
        total = vnf.groupby("date", as_index=False)["rh_mw_sum"].sum()
        last2 = total.sort_values("date").tail(2)["rh_mw_sum"]
        if len(last2) == 2 and last2.std(ddof=0) == 0:
            events.append({"stream": "vnf", "unit": "chaco_total",
                           "condition": "zero_variance_trailing_2m"})

    no2 = _read_or_empty(abs_path(p["data"]["s5p_monthly"]))
    if not no2.empty:
        no2["date"] = pd.to_datetime(no2["date"])
        for roi, g in no2.groupby("roi"):
            last2 = g.sort_values("date").tail(2)["no2_tropos_col_mol_m2"]
            if len(last2.dropna()) == 2 and last2.std(ddof=0) == 0:
                events.append({"stream": "s5p", "unit": roi,
                               "condition": "zero_variance_trailing_2m"})

    return events


def vnf_mom_drop_check(threshold: float = 0.30) -> list[dict]:
    p = paths()
    vnf = _read_or_empty(abs_path(p["data"]["vnf_monthly"]))
    if vnf.empty:
        return []
    vnf["date"] = pd.to_datetime(vnf["date"])
    total = vnf.groupby("date", as_index=False)["rh_mw_sum"].sum().sort_values("date")
    if len(total) < 2:
        return []
    last = total.iloc[-1]
    prev = total.iloc[-2]
    if prev["rh_mw_sum"] <= 0:
        return []
    mom = last["rh_mw_sum"] / prev["rh_mw_sum"] - 1
    if mom < -threshold:
        return [{
            "stream": "vnf", "unit": "chaco_total",
            "condition": f"mom_drop_{threshold:.0%}",
            "date": str(last["date"].date()),
            "mom_change": float(mom),
            "action": "Possible field maintenance OR real collapse. "
                      "La Linterna desk investigates within 48h.",
        }]
    return []


def viirs_yoy_drop_check(threshold: float = 0.15) -> list[dict]:
    p = paths()
    viirs = _read_or_empty(abs_path(p["data"]["viirs_sol_monthly"]))
    if viirs.empty:
        return []
    viirs["date"] = pd.to_datetime(viirs["date"])
    viirs = viirs[viirs["city"].isin(MAJOR_CITIES)].sort_values(["city", "date"])

    events: list[dict] = []
    for city, g in viirs.groupby("city"):
        g = g.copy()
        g["sol_yoy"] = g["sol"] / g["sol"].shift(12) - 1
        row = g.dropna(subset=["sol_yoy"]).tail(1)
        if row.empty:
            continue
        yoy = float(row["sol_yoy"].iloc[0])
        month = int(row["date"].iloc[0].month)
        low_cov = bool(row.get("low_coverage_flag", pd.Series([False])).iloc[0]) \
            if "low_coverage_flag" in row.columns else False
        if yoy < -threshold and month not in CLOUD_WINDOWS.get(city, set()) and not low_cov:
            events.append({
                "stream": "viirs", "unit": city,
                "condition": f"yoy_drop_{threshold:.0%}",
                "date": str(row["date"].iloc[0].date()),
                "yoy_change": yoy,
                "action": "Cross-check against city electricity demand.",
            })
    return events


def main() -> None:
    events = zero_variance_check() + vnf_mom_drop_check() + viirs_yoy_drop_check()
    halt = any(e.get("condition", "").startswith("zero_variance") for e in events)
    result = {
        "n_alerts": len(events),
        "events": events,
        "halt_publication": halt,
    }
    out = abs_path("data/satellite/pipeline_alerts.json")
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(result, indent=2, default=str))
    print(f"[ok] pipeline_alerts: n={len(events)} halt={halt} → {out}")


if __name__ == "__main__":
    main()
