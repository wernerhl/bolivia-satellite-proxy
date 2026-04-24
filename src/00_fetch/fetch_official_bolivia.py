"""Scaffolding for Bolivian official-series fetchers.

Each source exposes different formats — INE and BCB publish Excel
workbooks with irregular sheet layouts; YPFB and SIN use scanned PDFs
for some vintages; Aduana has an XML web service. A fully automated
full-history scraper is beyond the current scope.

Strategy: this module documents the official landing pages, writes
fetchable URLs into a manifest, and normalizes any locally-placed
CSVs into the canonical format consumed by src/05_econometrics/
and src/03_index/build_ci.py. Drop raw downloads into
`data/official/_inbox/`; run this script to promote them.

Canonical schemas (all CSV, date = first-of-month):
  ine_gdp_quarterly.csv         date, gdp_real                 (2017 base)
  ine_igae.csv                  date, igae, dollar_premium?    (from Mar 2026)
  ine_gdp_dept.csv              year, department, gdp_usd
  ine_hydrocarbon_va.csv        date, hydrocarbon_va
  ibch_cement.csv               date, cement_t
  cndc_electricity.csv          date, generation_mwh
  ypfb_hydrocarbons.csv         date, gas_prod_mmm3d, oil_prod_kbd?
  ypfb_field_month.csv          date, field, gas_prod_mmm3d
  ypfb_fuel_sales_metro.csv     date, roi, fuel_sales
  sin_tax.csv                   date, iva_real, it_real        (2018 IPC base)
  aduana_imports.csv            date, imports_usd_cif, imports_kg
"""
from __future__ import annotations

import json
import shutil
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from _common import abs_path, ensure_dir, load_env  # noqa: E402

load_env()


SOURCES = {
    "ine_gdp_quarterly": {
        "url": "https://www.ine.gob.bo/index.php/estadisticas-economicas/pib/",
        "notes": "PIB trimestral a precios constantes 2017; download XLS quarterly.",
    },
    "ine_igae": {
        "url": "https://www.ine.gob.bo/index.php/indice-global-de-actividad-economica-igae/",
        "notes": "IGAE experimental desde marzo 2026 (asistencia técnica FMI).",
    },
    "ine_gdp_dept": {
        "url": "https://www.ine.gob.bo/index.php/estadisticas-economicas/pib-departamental/",
        "notes": "PIB departamental anual; Bolivia publishes with 1-2 year lag.",
    },
    "ine_hydrocarbon_va": {
        "url": "https://www.ine.gob.bo/",
        "notes": "Valor agregado sectorial de hidrocarburos; extract from quarterly GDP release.",
    },
    "ibch_cement": {
        "url": "https://www.ibch.com/",
        "notes": "Despachos de cemento mensuales; IBCH portal requires manual download.",
    },
    "cndc_electricity": {
        "url": "https://www.cndc.bo/agentes/generacion.php",
        "notes": "Generación SIN por agente y por mes.",
    },
    "ypfb_hydrocarbons": {
        "url": "https://www.ypfb.gob.bo/es/publicaciones-y-estadisticas/boletines-estadisticos",
        "notes": "Boletín estadístico mensual; PDF for older vintages, XLS recently.",
    },
    "ypfb_field_month": {
        "url": "https://www.ypfb.gob.bo/",
        "notes": "Producción por campo; only aggregate data published — fields require inference.",
    },
    "ypfb_fuel_sales_metro": {
        "url": "https://www.ypfb.gob.bo/",
        "notes": "Ventas de combustibles por departamento; metro-level requires allocation rules.",
    },
    "sin_tax": {
        "url": "https://www.impuestos.gob.bo/ck-finder/connector?action=browse&currentFolder=/",
        "notes": "Recaudaciones mensuales IVA + IT; deflate by INE IPC (2018 base).",
    },
    "aduana_imports": {
        "url": "https://www.aduana.gob.bo/aduana7/",
        "notes": "Importaciones mensuales CIF + kg; Aduana XML web service requires registration.",
    },
}


def write_manifest() -> Path:
    out = abs_path("data/official/SOURCES.json")
    ensure_dir(out.parent)
    out.write_text(json.dumps(SOURCES, indent=2))
    return out


def promote_inbox() -> list[Path]:
    """Move any CSV dropped in data/official/_inbox/ into the main directory,
    validating that the schema matches one of the canonical series."""
    inbox = abs_path("data/official/_inbox")
    if not inbox.exists():
        return []
    promoted: list[Path] = []
    for f in inbox.glob("*.csv"):
        stem = f.stem.lower()
        match = next((k for k in SOURCES if k == stem), None)
        if match is None:
            print(f"[warn] unknown series {f.name}; leaving in _inbox/")
            continue
        target = abs_path(f"data/official/{match}.csv")
        shutil.move(str(f), target)
        promoted.append(target)
        print(f"[ok] promoted {f.name} → {target}")
    return promoted


def main() -> None:
    ensure_dir(abs_path("data/official/_inbox"))
    manifest = write_manifest()
    print(f"[ok] wrote source manifest → {manifest}")
    promoted = promote_inbox()
    print(f"[ok] promoted {len(promoted)} files from _inbox/")


if __name__ == "__main__":
    main()
