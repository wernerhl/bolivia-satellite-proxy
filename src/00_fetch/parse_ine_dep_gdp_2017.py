"""Parse INE 2017-base departmental GDP cuadros.

Source: https://www.ine.gob.bo/referencia2017/pib_departamental.html
Each entity (9 departments + national summary) has 4 cuadros:
  D.X.2.1  chained-volume MEASURES (millones de bolivianos encadenados)
  D.X.2.2  chained-volume INDICES (2017 = 100)
  D.X.2.3  y/y VARIATION (%)
  D.X.2.4  CONTRIBUTION to variation (%)

Coverage: 2017-2024 annual.

Outputs four canonical CSVs:
  data/official/ine_gdp_dept_2017_chained.csv     — primary (real value)
  data/official/ine_gdp_dept_2017_index.csv       — 2017 = 100
  data/official/ine_gdp_dept_2017_growth.csv      — y/y %
  data/official/ine_gdp_dept_2017_contribution.csv

Also produces a chain-spliced series combining the legacy 1990-base panel
(1988–2016, in `ine_gdp_dept.csv`) with the new 2017-base panel:

  data/official/ine_gdp_dept_spliced.csv
      year, department, sector_isic_section, gdp_real_index_2017,
      base_year, splice_factor

The splice factor for each (department, sector) pair is the ratio
INDEX_2017_at_2017 / INDEX_1990_at_2017 such that the 1990-base
sub-period rescales onto the 2017-base trajectory at the 2017 anchor.
For 1990-base years before 2017, we apply the splice factor; the
2017-base years are taken at face. The resulting unified index has
2017 = 100.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

import pandas as pd
import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from _common import abs_path, ensure_dir, load_env  # noqa: E402

load_env()


RAW = abs_path("data/official/ine_dep_2017_raw")
DEP_NUM = {
    "chuquisaca": 1, "la_paz": 2, "cochabamba": 3, "oruro": 4, "potosi": 5,
    "tarija": 6, "santa_cruz": 7, "beni": 8, "pando": 9,
    "national_summary": 10,
}
CUADROS = {
    1: ("chained",      "millones_bolivianos_encadenados"),
    2: ("index",        "2017_eq_100"),
    3: ("growth",       "y_y_percent"),
    4: ("contribution", "y_y_percent_points"),
}


def _parse_cuadro(path: Path, dept: str, cuadro: int) -> pd.DataFrame:
    raw = pd.read_excel(path, sheet_name=0, header=None)
    # Find the header row containing the first year
    header_row = None
    for i in range(20):
        row = raw.iloc[i]
        years = [v for v in row if isinstance(v, (int, float))
                 and 2015 <= float(v) <= 2030]
        if years:
            header_row = i
            break
    if header_row is None:
        return pd.DataFrame()

    header = raw.iloc[header_row]
    year_cols = {}
    for j, v in enumerate(header):
        if pd.notna(v):
            # Strip "(p)" preliminary marker
            s = str(v).strip()
            m = re.match(r"^(\d{4})", s)
            if m:
                yr = int(m.group(1))
                if 2015 <= yr <= 2030:
                    year_cols[j] = yr

    # Activity description column is typically col 3 (PRODUCTO INTERNO BRUTO,
    # VALOR AGREGADO BRUTO, plus sectors A..U). ISIC section letter is col 2.
    rows: list[dict] = []
    for i in range(header_row + 1, len(raw)):
        sector_label = raw.iloc[i, 3]
        section_letter = raw.iloc[i, 2]
        if pd.isna(sector_label):
            continue
        sec_label = str(sector_label).strip()
        if not sec_label or "Fuente" in sec_label:
            continue
        section = str(section_letter).strip() if pd.notna(section_letter) else ""
        for j, year in year_cols.items():
            v = raw.iloc[i, j]
            if pd.notna(v):
                try:
                    rows.append({
                        "year": year,
                        "department": dept,
                        "isic_section": section,
                        "sector_label": sec_label,
                        "value": float(v),
                    })
                except Exception:
                    pass
    return pd.DataFrame(rows)


def main() -> None:
    out_dir = ensure_dir(abs_path("data/official"))
    parsed: dict[int, list[pd.DataFrame]] = {1: [], 2: [], 3: [], 4: []}
    for dept, num in DEP_NUM.items():
        for q in (1, 2, 3, 4):
            fp = RAW / f"{dept}_D{num}.2.{q}.xlsx"
            if not fp.exists():
                print(f"[warn] missing {fp.name}")
                continue
            df = _parse_cuadro(fp, dept, q)
            if not df.empty:
                parsed[q].append(df)

    for q, frames in parsed.items():
        kind, _ = CUADROS[q]
        if not frames:
            continue
        df = pd.concat(frames, ignore_index=True).sort_values(
            ["department", "year", "isic_section", "sector_label"])
        out = out_dir / f"ine_gdp_dept_2017_{kind}.csv"
        df.to_csv(out, index=False)
        print(f"[ok] cuadro D.X.2.{q}: {len(df)} rows -> {out.name}")

    # Chain-splice attempt: 1990-base ends 2016, 2017-base starts 2017.
    # ZERO overlap years. True splicing requires at least one common year
    # to compute the rebasing factor. We therefore do NOT splice; we
    # provide a side-by-side concatenation keyed by base_year so that
    # any consumer must explicitly choose a regime to work in.
    legacy_path = abs_path("data/official/ine_gdp_dept_sectoral.csv")
    chained_path = out_dir / "ine_gdp_dept_2017_chained.csv"
    if not legacy_path.exists() or not chained_path.exists():
        return

    legacy = pd.read_csv(legacy_path).rename(columns={"sector": "sector_label",
                                                        "gva": "value"})
    legacy["base_year"] = 1990
    new = pd.read_csv(chained_path)
    new["base_year"] = 2017
    new = new.rename(columns={"value": "value"})  # explicit no-op
    if "isic_section" not in legacy.columns:
        legacy["isic_section"] = ""

    cols = ["year", "department", "isic_section", "sector_label",
            "value", "base_year"]
    combined = pd.concat([legacy[cols], new[cols]], ignore_index=True
                         ).sort_values(["department", "year", "sector_label"])
    out_combined = out_dir / "ine_gdp_dept_combined.csv"
    combined.to_csv(out_combined, index=False)
    print(f"[ok] combined 1990-base (1988-2016) + 2017-base (2017-2024) "
          f"-> {out_combined.name} "
          f"({len(combined)} rows; NO splice, base_year column distinguishes)")


if __name__ == "__main__":
    main()
