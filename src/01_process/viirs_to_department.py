"""Aggregate city-buffer VIIRS monthly SOL into annual departmental totals.

Mapping from the 11 urban buffers to Bolivia's 9 departments. The VIIRS
city buffers do not cover rural Bolivia, so the departmental SOL is
really "major-urban lighting in department d"; this is what the HSW-
style regression in Eq.~(1) uses as the right-hand-side variable.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from _common import abs_path, load_env, paths  # noqa: E402

load_env()


CITY_TO_DEPT = {
    "la_paz_el_alto": "la_paz",
    "santa_cruz":     "santa_cruz",
    "cochabamba":     "cochabamba",
    "sucre":          "chuquisaca",
    "oruro":          "oruro",
    "potosi":         "potosi",
    "tarija":         "tarija",
    "trinidad":       "beni",
    "cobija":         "pando",
    "montero":        "santa_cruz",
    "yacuiba":        "tarija",
}


def main() -> None:
    p = paths()
    monthly = pd.read_csv(abs_path(p["data"]["viirs_sol_monthly"]),
                          parse_dates=["date"])
    monthly = monthly.dropna(subset=["sol"]).copy()
    if "low_coverage_flag" in monthly.columns:
        monthly = monthly[~monthly["low_coverage_flag"].astype(bool)]
    monthly["department"] = monthly["city"].map(CITY_TO_DEPT)
    monthly["year"] = monthly["date"].dt.year

    # Annual per-department SOL = sum of city-monthly SOL over the year
    annual = (monthly.groupby(["year", "department"], as_index=False)["sol"]
              .sum()
              .rename(columns={"sol": "sol"}))
    out = abs_path("data/satellite/viirs_sol_dept_annual.csv")
    out.parent.mkdir(parents=True, exist_ok=True)
    annual.to_csv(out, index=False)
    n_dep = annual["department"].nunique()
    n_year = annual["year"].nunique()
    print(f"[ok] wrote {out} ({len(annual)} rows, "
          f"{n_dep} departments, {n_year} years)")


if __name__ == "__main__":
    main()
