"""Parse 9 INE departmental-GDP xlsx + the national summary xlsx into
two canonical CSVs:

  data/official/ine_gdp_dept.csv
      year, department, gdp_real
      9 deps × 37 years = 333 rows, 1988--2024, 1990 constant prices.

  data/official/ine_gdp_dept_sectoral.csv
      year, department, sector, gva
      long-format sectoral panel (used for the Track A NDVI elasticity).
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from _common import abs_path, ensure_dir, load_env  # noqa: E402

load_env()


RAW_DIR = abs_path("data/official/ine_dep_raw")
DEPARTMENTS = [
    "chuquisaca", "la_paz", "cochabamba", "oruro", "potosi",
    "tarija", "santa_cruz", "beni", "pando",
]


def _parse_dep_xlsx(path: Path) -> pd.DataFrame:
    """Return a long-format DataFrame (year, sector, gva) for one department."""
    raw = pd.read_excel(path, sheet_name=0, header=None)

    # Find the header row that contains the first year (a numeric 1988).
    header_row = None
    for i in range(20):
        row = raw.iloc[i]
        if any(isinstance(v, (int, float)) and 1985 <= float(v) <= 2030
               for v in row if pd.notna(v)):
            header_row = i
            break
    if header_row is None:
        return pd.DataFrame()

    header = raw.iloc[header_row]
    year_cols = {i: int(v) for i, v in enumerate(header)
                 if pd.notna(v) and isinstance(v, (int, float))
                 and 1985 <= float(v) <= 2030}
    sector_col = 1  # column B holds sector/activity name
    rows: list[dict] = []
    for i in range(header_row + 1, len(raw)):
        label = raw.iloc[i, sector_col]
        if pd.isna(label):
            continue
        sector = str(label).strip()
        if "Fuente" in sector or "(p)" == sector:
            continue
        for col_idx, year in year_cols.items():
            val = raw.iloc[i, col_idx]
            if pd.notna(val):
                try:
                    rows.append({"year": year, "sector": sector,
                                 "gva": float(val)})
                except Exception:
                    pass
    return pd.DataFrame(rows)


def main() -> None:
    ensure_dir(abs_path("data/official"))

    dep_frames: list[pd.DataFrame] = []
    dept_total_rows: list[dict] = []
    for name in DEPARTMENTS:
        fp = RAW_DIR / f"{name}.xlsx"
        if not fp.exists():
            print(f"[warn] missing {fp}")
            continue
        df = _parse_dep_xlsx(fp)
        df["department"] = name
        dep_frames.append(df)
        total_sect = df[df["sector"].str.contains(
            "PRODUCTO INTERNO BRUTO", case=False, na=False)]
        for _, r in total_sect.iterrows():
            dept_total_rows.append({
                "year": int(r["year"]),
                "department": name,
                "gdp_real": float(r["gva"]),
            })
        print(f"[ok] {name}: {len(df)} sectoral rows, "
              f"{len(total_sect)} total-GDP rows")

    if dep_frames:
        sectoral = pd.concat(dep_frames, ignore_index=True).sort_values(
            ["department", "year", "sector"])
        sp = abs_path("data/official/ine_gdp_dept_sectoral.csv")
        sectoral.to_csv(sp, index=False)
        print(f"[ok] wrote {sp} ({len(sectoral)} rows)")

        totals = pd.DataFrame(dept_total_rows).drop_duplicates(
            subset=["year", "department"], keep="first"
        ).sort_values(["department", "year"])
        tp = abs_path("data/official/ine_gdp_dept.csv")
        totals.to_csv(tp, index=False)
        print(f"[ok] wrote {tp} ({len(totals)} rows, "
              f"{totals['year'].min()}--{totals['year'].max()}, "
              f"{totals['department'].nunique()} departments)")


if __name__ == "__main__":
    main()
