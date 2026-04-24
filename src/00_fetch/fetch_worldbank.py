"""World Bank WDI annual series for Bolivia (no auth, public API).

Used as a sanity benchmark for the satellite-implied annual growth and
for the HSW-style elasticity regression on departmental GDP (when
departmental data eventually lands).

Indicators:
  NY.GDP.MKTP.KN       Real GDP, constant LCU
  NY.GDP.MKTP.KD.ZG    Annual real GDP growth (%)
  FI.RES.TOTL.CD       Total reserves (USD)
  NE.IMP.GNFS.CD       Imports of goods and services (USD)
  NY.GDP.PETR.RT.ZS    Petroleum rents, % of GDP
  TX.VAL.FUEL.ZS.UN    Fuel exports (% of merchandise exports)
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd
import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from _common import abs_path, ensure_dir, load_env  # noqa: E402

load_env()


INDICATORS = {
    "gdp_real_lcu": "NY.GDP.MKTP.KN",
    "gdp_growth_pct": "NY.GDP.MKTP.KD.ZG",
    "reserves_usd": "FI.RES.TOTL.CD",
    "imports_usd": "NE.IMP.GNFS.CD",
    "petroleum_rents_pct_gdp": "NY.GDP.PETR.RT.ZS",
    "fuel_exports_pct": "TX.VAL.FUEL.ZS.UN",
}


def fetch(indicator: str) -> pd.DataFrame:
    url = (f"https://api.worldbank.org/v2/country/BOL/indicator/{indicator}"
           f"?format=json&date=2000:2026&per_page=100")
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    rows = r.json()[1] or []
    return pd.DataFrame([{"year": int(x["date"]), "value": x["value"]}
                         for x in rows if x["value"] is not None]).sort_values("year")


def main() -> None:
    out_dir = ensure_dir(abs_path("data/official"))
    frames = {}
    for name, indicator in INDICATORS.items():
        try:
            df = fetch(indicator)
            frames[name] = df
            print(f"[ok] {name} ({indicator}): {len(df)} years")
        except Exception as e:
            print(f"[..] {name}: {e}")

    # Wide panel
    if frames:
        merged = None
        for name, df in frames.items():
            d = df.rename(columns={"value": name})
            merged = d if merged is None else merged.merge(d, on="year", how="outer")
        merged = merged.sort_values("year")
        out = out_dir / "worldbank_bolivia_annual.csv"
        merged.to_csv(out, index=False)
        print(f"[ok] wrote {out} ({len(merged)} years, {len(frames)} indicators)")


if __name__ == "__main__":
    main()
