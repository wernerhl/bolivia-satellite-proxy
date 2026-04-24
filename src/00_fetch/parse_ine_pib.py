"""Parse INE quarterly PIB xlsx into canonical CSVs.

Source: 23 files under data/official/ine_raw/, downloaded from the INE
Nimbus (Nextcloud) share. Only a subset is consumed by the pipeline;
the rest are preserved for manual inspection.

Mapping (INE cuadro code → canonical series):
  01.01.01  "PIB por actividad económica, a precios constantes de 1990"
            → ine_gdp_quarterly.csv (total + sectoral)
            → ine_hydrocarbon_va.csv (Petróleo crudo y gas natural column)

The INE workbooks put a single wide table on sheet 1 with an 8-row
header block. Column names live on row 10 (0-indexed 9); row labels
in column 0 alternate yearly totals with quarterly breakouts
("I Trimestre", "II Trimestre", ...).
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from _common import abs_path, ensure_dir, load_env  # noqa: E402

load_env()


QUARTER_MAP = {
    "I Trimestre": 1, "II Trimestre": 2, "III Trimestre": 3, "IV Trimestre": 4,
    "I   Trimestre": 1, "II  Trimestre": 2, "III Trimestre": 3, "IV  Trimestre": 4,
}


def _normalize(s: str) -> str:
    return re.sub(r"\s+", " ", str(s).strip())


def parse_cuadro_01_01_01(path: Path) -> pd.DataFrame:
    raw = pd.read_excel(path, sheet_name=0, header=None)
    # Find header row containing "PERIODO"
    header_row = None
    for i in range(20):
        row = raw.iloc[i].astype(str).str.strip().str.upper()
        if row.str.contains("PERIODO", na=False).any():
            header_row = i
            break
    if header_row is None:
        raise RuntimeError(f"no PERIODO header found in {path.name}")

    df = pd.read_excel(path, sheet_name=0, header=header_row)
    df.columns = [_normalize(c) for c in df.columns]
    periodo_col = next(c for c in df.columns if "PERIODO" in c.upper())

    current_year: int | None = None
    rows: list[dict] = []
    for _, r in df.iterrows():
        label = _normalize(str(r[periodo_col]))
        # Year rows: four-digit year optionally followed by (p) for preliminary.
        m = re.fullmatch(r"(\d{4})(?:\s*\(p\))?", label)
        if m:
            current_year = int(m.group(1))
            continue
        norm = _normalize(label).replace("   ", " ").replace("  ", " ")
        if norm in QUARTER_MAP and current_year is not None:
            q = QUARTER_MAP[norm]
            month = (q - 1) * 3 + 1
            date = pd.Timestamp(year=current_year, month=month, day=1)
            rec = {"date": date, "year": current_year, "quarter": q}
            for c in df.columns:
                if c == periodo_col:
                    continue
                val = r[c]
                if pd.notna(val):
                    try:
                        rec[c] = float(val)
                    except Exception:
                        pass
            rows.append(rec)
    return pd.DataFrame(rows)


def main() -> None:
    raw_dir = abs_path("data/official/ine_raw")
    out_dir = ensure_dir(abs_path("data/official"))

    src_01_01_01 = raw_dir / "01_01.01.01.xlsx"
    if not src_01_01_01.exists():
        raise RuntimeError(f"missing: {src_01_01_01}")

    df = parse_cuadro_01_01_01(src_01_01_01)
    # Locate the total column and hydrocarbon sectoral column
    gdp_cols = [c for c in df.columns if "PIB A PRECIOS DE MERCADO" in c.upper()]
    hydro_cols = [c for c in df.columns if "PETRÓLEO" in c.upper() and "GAS" in c.upper()]

    # Canonical quarterly GDP total
    if gdp_cols:
        col = gdp_cols[0]
        gdp = df[["date", col]].rename(columns={col: "gdp_real"}).sort_values("date")
        gdp_path = out_dir / "ine_gdp_quarterly.csv"
        gdp.to_csv(gdp_path, index=False)
        print(f"[ok] quarterly GDP → {gdp_path} ({len(gdp)} rows, "
              f"{gdp['date'].min().date()} .. {gdp['date'].max().date()})")

    # Hydrocarbon VA
    if hydro_cols:
        col = hydro_cols[0]
        hva = df[["date", col]].rename(columns={col: "hydrocarbon_va"}).sort_values("date")
        hva_path = out_dir / "ine_hydrocarbon_va.csv"
        hva.to_csv(hva_path, index=False)
        print(f"[ok] hydrocarbon VA → {hva_path} ({len(hva)} rows)")

    # Full sectoral panel for downstream needs
    full_path = out_dir / "ine_gdp_quarterly_sectoral.csv"
    df.to_csv(full_path, index=False)
    print(f"[ok] full sectoral → {full_path} ({len(df)} rows, {len(df.columns)} cols)")


if __name__ == "__main__":
    main()
