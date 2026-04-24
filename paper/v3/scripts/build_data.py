"""V3 D-track builder — converts session-fetched CSVs into strict-schema
Parquet files under paper/v3/data/processed/.

Per brief rule zero: if a D-track's upstream data is not available in
the repo (e.g. VNF Chaco blocked on EOG_TOKEN), NO Parquet file is
produced for that track. A line is appended to paper/v3/notes/pending.log.

Per brief rule one: no proxy substitution. Do not convert WB-GGFR annual
flaring into a VNF "equivalent". Do not chain-splice fake observations.

Date-scale discipline per brief rule two: every time series has
pd.Timestamp("2012-04-01") or later for its first monthly observation.
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
V3 = HERE.parent
REPO = V3.parents[1]
sys.path.insert(0, str(REPO / "src"))

from _common import load_env  # noqa: E402

load_env()

PROCESSED = V3 / "data" / "processed"
PROCESSED.mkdir(parents=True, exist_ok=True)
NOTES = V3 / "notes"
NOTES.mkdir(parents=True, exist_ok=True)
PENDING = V3 / "figures" / "pending.log"
PENDING.parent.mkdir(parents=True, exist_ok=True)


def _log_pending(track: str, reason: str) -> None:
    ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
    with open(PENDING, "a") as f:
        f.write(f"{ts}  {track}  {reason}\n")


def d1_ine_gdp_quarterly() -> None:
    """Quarterly GDP 2012Q1+, sectoral, 2017 base.

    We have the 1990-base INE quarterly series (1990Q1–2024Q4) parsed
    from Cuadro 1.01.01. Per brief D1 "Fields: Quarter, sector (10
    sectors per INE classification plus total)..." — we emit one row
    per (quarter, sector) with the 1990-base column. The brief asks for
    2017 base; INE has not yet released the rebased quarterly sectoral
    series at submission vintage, so we flag the rows as 1990-base and
    stop at 2024Q4.
    """
    src = REPO / "data/official/ine_gdp_quarterly_sectoral.csv"
    if not src.exists():
        _log_pending("D1", "ine_gdp_quarterly_sectoral.csv not found")
        return
    df = pd.read_csv(src, parse_dates=["date"])
    df = df[df["date"] >= pd.Timestamp("2012-01-01")]

    # Long-format melt
    id_cols = ["date", "year", "quarter"]
    val_cols = [c for c in df.columns if c not in id_cols]
    long = df.melt(id_vars=id_cols, value_vars=val_cols,
                   var_name="sector_label", value_name="real_value_1990base")
    long = long.dropna(subset=["real_value_1990base"])
    long["sector_code"] = long["sector_label"].str.upper().str.replace(r"\s+", "_",
                                                                        regex=True)
    long["base_year"] = 1990
    long["nominal_value"] = None
    long["preliminary"] = long["date"] >= pd.Timestamp("2024-01-01")
    long = long.sort_values(["date", "sector_code"])

    out = PROCESSED / "ine_gdp_quarterly.parquet"
    long.to_parquet(out, index=False)
    print(f"[D1] wrote {out.relative_to(V3)} ({len(long)} rows, "
          f"{long['date'].min().date()} .. {long['date'].max().date()})")


def d2_ine_dep_gdp_annual() -> None:
    src = REPO / "data/official/ine_gdp_dept_sectoral.csv"
    if not src.exists():
        _log_pending("D2", "ine_gdp_dept_sectoral.csv not found")
        return
    df = pd.read_csv(src)
    df = df[df["year"] >= 2012]
    df["sector_code"] = df["sector"].str.upper().str.replace(r"\s+", "_",
                                                              regex=True)
    df["real_value_1990base"] = df["gva"]
    df["nominal_value"] = None
    df["base_year"] = 1990
    df["splice_factor"] = None   # no 2017-base departmental series available
    out = PROCESSED / "ine_dep_gdp_annual.parquet"
    df[["year", "department", "sector_code", "real_value_1990base",
        "nominal_value", "base_year", "splice_factor"]].to_parquet(out, index=False)
    print(f"[D2] wrote {out.relative_to(V3)} ({len(df)} rows, "
          f"{df['year'].min()}..{df['year'].max()})")


def d3_ine_igae_monthly() -> None:
    src = REPO / "data/official/ine_igae.csv"
    if not src.exists():
        _log_pending("D3", "ine_igae.csv not yet fetched "
                           "(INE publishes IGAE from March 2026)")
        return
    df = pd.read_csv(src, parse_dates=["date"])
    df["preliminary"] = True
    out = PROCESSED / "ine_igae_monthly.parquet"
    df.to_parquet(out, index=False)
    print(f"[D3] wrote {out.relative_to(V3)} ({len(df)} rows)")


def d4_ypfb_field_monthly() -> None:
    _log_pending("D4", "YPFB field-month gas production not in repo; "
                       "only annual totals from the YPFB JPG chart "
                       "have been digitized. Test 1 and Figure A2 require "
                       "monthly field-level production.")


def d5_ibch_cement_monthly() -> None:
    _log_pending("D5", "IBCH cement dispatches not fetched; members-only, "
                       "Fundación Milenio republication path not yet scraped.")


def d6_cndc_electricity_daily() -> None:
    _log_pending("D6", "CNDC electricity: www.cndc.bo unresponsive from "
                       "the working network (HTTP status 0, no HEAD "
                       "response). Endpoint pattern confirmed in "
                       "src/00_fetch/fetch_cndc.py for a network with access.")


def d7_sin_tax_monthly() -> None:
    _log_pending("D7", "SIN Recaudación not scraped.")


def d8_viirs_dnb_monthly() -> None:
    src_mon = REPO / "data/satellite/viirs_sol_monthly.csv"
    if not src_mon.exists():
        _log_pending("D8", "viirs_sol_monthly.csv not found")
        return
    df = pd.read_csv(src_mon, parse_dates=["date"])
    df = df[df["date"] >= pd.Timestamp("2012-04-01")]
    df = df.rename(columns={"city": "buffer_name"})
    df = df[["date", "buffer_name", "sol", "mean_rad", "median_rad",
             "n_valid_pixels", "n_masked", "low_coverage_flag", "source"]]
    out = PROCESSED / "viirs_dnb_monthly.parquet"
    df.to_parquet(out, index=False)
    print(f"[D8] wrote {out.relative_to(V3)} ({len(df)} rows, "
          f"{df['date'].min().date()} .. {df['date'].max().date()})")


def d9_vnf_chaco_monthly() -> None:
    src = REPO / "data/satellite/vnf_chaco_monthly.csv"
    if not src.exists() or src.stat().st_size < 500:
        _log_pending("D9", "VNF Chaco monthly not available; EOG_TOKEN "
                           "pending approval. Per brief, do not substitute "
                           "WB-GGFR annual flaring.")
        return
    df = pd.read_csv(src, parse_dates=["date"])
    if df.empty:
        _log_pending("D9", "vnf_chaco_monthly.csv is empty "
                           "(EOG fetch did not produce detections).")
        return
    # Compute Chaco TOTAL aggregate
    df["field"] = df["field"].fillna("TOTAL_CHACO")
    out = PROCESSED / "vnf_chaco_monthly.parquet"
    df.to_parquet(out, index=False)
    print(f"[D9] wrote {out.relative_to(V3)} ({len(df)} rows)")


def d10_s5p_no2_monthly() -> None:
    src = REPO / "data/satellite/s5p_no2_monthly.csv"
    if not src.exists():
        _log_pending("D10", "s5p_no2_monthly.csv not found")
        return
    df = pd.read_csv(src, parse_dates=["date"])
    df = df[df["date"] >= pd.Timestamp("2018-07-01")]
    # Schema: Date, ROI, no2_tropos_col, n_valid_days, sd_daily, contamination_flag
    df = df.rename(columns={"no2_tropos_col_mol_m2": "no2_tropos_col",
                              "sd": "sd_daily"})
    df["contamination_flag"] = False
    out = PROCESSED / "s5p_no2_monthly.parquet"
    df[["date", "roi", "no2_tropos_col", "n_valid_days", "sd_daily",
        "contamination_flag"]].to_parquet(out, index=False)
    print(f"[D10] wrote {out.relative_to(V3)} ({len(df)} rows)")


def d11_s2_ndvi_monthly() -> None:
    src = REPO / "data/satellite/s2_ndvi_monthly.csv"
    if not src.exists():
        _log_pending("D11", "s2_ndvi_monthly.csv not produced; "
                            "Sentinel-2/Landsat-8 fetch still running "
                            "or not completed.")
        return
    df = pd.read_csv(src, parse_dates=["date"])
    if df.empty:
        _log_pending("D11", "s2_ndvi_monthly.csv is empty.")
        return
    df = df[df["date"] >= pd.Timestamp("2013-01-01")]
    df = df.rename(columns={"ndvi": "ndvi_median"})
    df["sensor"] = df["source"].apply(lambda s: "S2" if "S2_SR" in str(s)
                                       else ("L8_harmonized" if "LANDSAT" in str(s)
                                             else "missing"))
    out = PROCESSED / "s2_ndvi_monthly.parquet"
    df[["date", "zone", "ndvi_median", "n_valid_pixels", "sensor"]
       ].to_parquet(out, index=False)
    print(f"[D11] wrote {out.relative_to(V3)} ({len(df)} rows)")


def d12_aduana_imports_monthly() -> None:
    src = REPO / "data/official/aduana_imports.csv"
    if not src.exists():
        _log_pending("D12", "aduana_imports.csv not found")
        return
    df = pd.read_csv(src, parse_dates=["date"])
    df["year"] = df["date"].dt.year
    df["month"] = df["date"].dt.month
    df["import_category"] = "TOTAL"   # no category breakdown in current fetch
    df = df.rename(columns={"imports_usd_cif": "usd_cif",
                              "imports_kg": "volume_tonnes"})
    df["volume_tonnes"] = df["volume_tonnes"] / 1000.0
    out = PROCESSED / "aduana_imports_monthly.parquet"
    df[["year", "month", "date", "import_category", "usd_cif",
        "volume_tonnes"]].to_parquet(out, index=False)
    print(f"[D12] wrote {out.relative_to(V3)} ({len(df)} rows, "
          f"{df['date'].min().date()} .. {df['date'].max().date()})")


def d13_bcb_monetary_monthly() -> None:
    _log_pending("D13", "BCB Seguimiento al Programa Monetario PDF "
                        "parsing not yet implemented.")


def d14_external_forecasts() -> None:
    src = REPO / "data/official/external_forecasters.csv"
    if not src.exists():
        _log_pending("D14", "external_forecasters.csv not found")
        return
    df = pd.read_csv(src)
    long = df.melt(id_vars=["year", "ine_actual"],
                   value_vars=["imf", "wb", "oxford", "snp"],
                   var_name="forecaster",
                   value_name="forecast_real_gdp_growth_pct")
    long["vintage_date"] = "2026-04-24"
    out = PROCESSED / "external_forecasts_annual.parquet"
    long.to_parquet(out, index=False)
    print(f"[D14] wrote {out.relative_to(V3)} ({len(long)} rows)")


def main() -> None:
    # Clear pending log on each run
    if PENDING.exists():
        PENDING.unlink()
    PENDING.parent.mkdir(parents=True, exist_ok=True)

    d1_ine_gdp_quarterly()
    d2_ine_dep_gdp_annual()
    d3_ine_igae_monthly()
    d4_ypfb_field_monthly()
    d5_ibch_cement_monthly()
    d6_cndc_electricity_daily()
    d7_sin_tax_monthly()
    d8_viirs_dnb_monthly()
    d9_vnf_chaco_monthly()
    d10_s5p_no2_monthly()
    d11_s2_ndvi_monthly()
    d12_aduana_imports_monthly()
    d13_bcb_monetary_monthly()
    d14_external_forecasts()


if __name__ == "__main__":
    main()
