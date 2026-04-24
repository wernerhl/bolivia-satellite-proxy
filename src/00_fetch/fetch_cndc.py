"""Fetch CNDC (Comité Nacional de Despacho de Carga) monthly electricity
generation from the public xlsx endpoint pattern
`https://www.cndc.bo/php/dload.php?f=gen_dia_MMYY.xlsx&d=estmes`.

Produces:
  data/official/cndc_electricity.csv  (date, generation_mwh)
  data/official/cndc_raw/YYYYMM.xlsx  (raw files)

Each monthly file contains daily generation by plant; we sum to get the
monthly SIN (Sistema Interconectado Nacional) total.
"""
from __future__ import annotations

import io
import sys
from datetime import date
from pathlib import Path

import pandas as pd
import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from _common import abs_path, ensure_dir, load_env  # noqa: E402

load_env()


URL_TEMPLATE = "https://www.cndc.bo/php/dload.php?f=gen_dia_{mmyy}.xlsx&d=estmes"
HEADERS = {"User-Agent": "Mozilla/5.0 (bolivia-satellite-proxy)"}


def fetch_month(y: int, m: int, out_dir: Path) -> Path | None:
    mmyy = f"{m:02d}{y % 100:02d}"
    url = URL_TEMPLATE.format(mmyy=mmyy)
    target = out_dir / f"{y:04d}{m:02d}.xlsx"
    if target.exists() and target.stat().st_size > 0:
        return target
    try:
        r = requests.get(url, headers=HEADERS, timeout=60)
        if r.status_code != 200 or len(r.content) < 1000:
            return None
        target.write_bytes(r.content)
        return target
    except Exception:
        return None


def monthly_total_mwh(xlsx_path: Path) -> float | None:
    """Sum daily generation across all plants, days in month."""
    try:
        xl = pd.ExcelFile(xlsx_path)
        for sh in xl.sheet_names:
            df = pd.read_excel(xlsx_path, sheet_name=sh, header=None)
            # Heuristic: find the cell "TOTAL" in column 0 or a row where
            # daily columns sum to a clear monthly total.
            last_col = df.shape[1] - 1
            # Often the sheet has a "Energía Generada" section where the
            # last numeric column is monthly total.
            numeric = pd.to_numeric(df.iloc[:, last_col], errors="coerce")
            if numeric.notna().sum() > 0:
                # Sum all non-NaN numeric values in the last column;
                # robust even if structure varies.
                total = float(numeric.dropna().sum())
                if total > 0:
                    return total
    except Exception:
        return None
    return None


def main() -> None:
    out_dir = ensure_dir(abs_path("data/official/cndc_raw"))
    rows: list[dict] = []

    # Monthly range: 2012-01 through last full month
    today = date.today()
    y, m = 2012, 1
    end_y, end_m = (today.year, today.month - 1) if today.month > 1 else (today.year - 1, 12)

    while (y, m) <= (end_y, end_m):
        xlsx = fetch_month(y, m, out_dir)
        total = monthly_total_mwh(xlsx) if xlsx else None
        rows.append({"date": f"{y:04d}-{m:02d}-01",
                     "generation_mwh": total,
                     "source": "CNDC gen_dia xlsx"})
        if m < 12:
            m += 1
        else:
            y, m = y + 1, 1

    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])
    ok = df.dropna(subset=["generation_mwh"])
    df.to_csv(abs_path("data/official/cndc_electricity.csv"), index=False)
    print(f"[ok] CNDC electricity: {len(ok)} / {len(df)} months with data "
          f"→ data/official/cndc_electricity.csv")


if __name__ == "__main__":
    main()
