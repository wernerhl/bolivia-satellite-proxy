# Bolivia Satellite-Proxy Activity Pipeline

*Fires, Lights, and Smog: Reading Bolivia's Recession from Space* — end-to-end
pipeline and paper draft.

Four independent satellite-derived monthly activity proxies for Bolivia
— VIIRS DNB over urban buffers, VIIRS Nightfire over Chaco gas fields,
Sentinel-5P TROPOMI NO₂ over the two major metros, and Sentinel-2 NDVI
over five cropland zones — combined into a coincident indicator of real
activity and tested against INE national accounts.

**Contact:** [wernerhl@gmail.com](mailto:wernerhl@gmail.com).
No institutional affiliations.

---

## Directory layout

```
whl.BoliviaSatellite/
├── README.md                     this file
├── Makefile                      `make all` / `make paper-v2`
├── requirements.txt              Python dependencies
├── .env / .env.example           credentials (NEVER commit .env)
├── agent_prompts/                LOCAL ONLY — gitignored (§ Agent instructions)
│
├── config/                       YAML definitions (buffers, flares, ROIs, zones)
│   ├── paths.yaml                canonical file paths + stream params
│   ├── buffers.yaml              11 urban buffers for VIIRS DNB
│   ├── flares.yaml               7 Chaco gas-field centroids
│   ├── rois.yaml                 3 TROPOMI NO₂ rectangles
│   └── ndvi_zones.yaml           5 Sentinel-2 cropland zones
│
├── src/                          pipeline code (versioned jointly; runs for both v1 and v2)
│   ├── _common.py                shared utilities (env loading, EE auth, paths)
│   ├── 00_fetch/                 raw-data acquisition scripts
│   ├── 01_process/               VNF flare attribution, VIIRS→dept aggregation
│   ├── 02_anomaly/               per-stream deseasonalization + anomaly + z-score
│   ├── 03_index/                 CI construction + DuckDB + benchmark + alerts
│   ├── 04_publish/               weekly brief + monthly report + dashboard vintage
│   ├── 05_econometrics/          elasticities + DFM + BBQ/Markov + manipulation tests
│   ├── 06_paper/                 v1-vintage fill_paper patcher + legacy figures/tables
│   └── 99_validate/              quarterly HSW / VNF–YPFB / S5P–fuel validations
│
├── scripts/                      Track H paper-v2 deliverables
│   ├── figures/
│   │   ├── lalinterna_style.py   palette + matplotlib rcParams
│   │   └── make_all_figures.py   5 main + 2 appendix figures
│   └── tables/
│       └── make_all_tables.py    4 main + 2 appendix booktabs tables
│
├── data/
│   ├── satellite/                pipeline outputs (CSVs, JSONs, DuckDB)
│   │   ├── viirs_sol_monthly.csv          1 837 rows (11 cities × 167 months)
│   │   ├── viirs_sol_anomaly.csv          STL-SA, linear-trend anomaly
│   │   ├── viirs_sol_dept_annual.csv      9 depts × 15 years (for Eq. (1))
│   │   ├── s5p_no2_monthly.csv            3 ROIs × 94 months
│   │   ├── s5p_no2_anomaly.csv            multiplicative + z vs 2019
│   │   ├── bolivia_satellite_ci.csv       169 monthly observations
│   │   ├── dfm_twofactor_result.json      two-factor (urban + extractive) composite
│   │   ├── recession_dating.json          BBQ peaks/troughs + Markov probs
│   │   ├── manipulation_tests.json        3-test ladder with identifying assumptions
│   │   └── elasticity_*.json              single-series HAC β̂ per stream
│   └── official/                 external data, fetched or manually placed
│       ├── ine_gdp_quarterly.csv          1990 Q1–2024 Q4 (1990 base, INE)
│       ├── ine_gdp_dept.csv               9 depts × 29 years (1988-2016)
│       ├── ine_gdp_dept_sectoral.csv      full sectoral panel (8 791 rows)
│       ├── ine_hydrocarbon_va.csv         hydrocarbon VA, quarterly
│       ├── ypfb_gas_production_annual.csv 2006–2025 (from YPFB JPG)
│       ├── ypfb_hydrocarbons.csv          monthly expansion of annual
│       ├── aduana_imports.csv             Jan 2024 – Feb 2026 (CIF + FOB + KG)
│       ├── worldbank_bolivia_annual.csv   25 years × 6 WDI indicators
│       ├── wb_ggfr_bolivia_annual.csv     2012–2024 flaring BCM
│       ├── wb_ggfr_bolivia_flares.csv     113 Chaco-bbox-filtered flares
│       ├── external_forecasters.csv       IMF/WB/Oxford/S&P growth 2020–2026
│       ├── dollar_premium.csv             Binance P2P BOB/USD daily
│       ├── ine_raw/                       22 INE PIB xlsx + 1 methodology PDF
│       ├── ine_dep_raw/                   10 INE departmental xlsx
│       ├── aduana_raw/                    3 INE imports xlsx (annual microdata)
│       └── ypfb_charts/                   6 YPFB indicator JPGs
│
├── paper/
│   ├── v1/                       pre-revision snapshot (pristine baseline)
│   │   ├── fires_lights_smog.tex          14-section draft with TBD stubs
│   │   ├── fires_lights_smog.pdf          15 pages, 252 KB
│   │   ├── references.bib
│   │   ├── figures/                       auto-generated v1-vintage PDFs
│   │   └── tables/                        auto-generated v1-vintage .tex
│   └── v2/                       Track A–H revision, submission-ready
│       ├── fires_lights_smog.tex          coincident-indicator frame, 4 streams
│       ├── fires_lights_smog.pdf          21 pages, 363 KB
│       ├── references.bib                 with Bolívar-Cuba, Gao, Johnson, Donohue,
│       │                                  Donaldson-Storeygard, Roy, LMST added
│       ├── figures/pdf/                   5 main + 2 appendix figures @ 400 dpi
│       ├── figures/png/                   150-dpi mirrors for dashboard use
│       └── tables/                        6 booktabs fragments
│
├── outputs/
│   ├── dfm/bolivia_satellite_ci.parquet   clean DFM-module export
│   ├── dashboard/vintage.json             with per-stream latest date + git commit
│   ├── dashboard/vintage_archive/*.json   dated history (never-overwrite)
│   └── zenodo/                            frozen replication bundle (9–11 series)
│
├── tests/                        pytest smoke tests (config loads, 11 cities,
│                                 7 flares, 3 ROIs, weights sum to 1)
└── logs/                         background-fetch stderr/stdout
```

### Versioning rule

Pipeline code in `src/` and `scripts/` produces both v1 and v2 paper
artifacts. The paper itself lives in `paper/v1/` (pristine pre-revision
baseline) and `paper/v2/` (Track A–H revision against the internal critique).
To rebuild:

```
make paper-v1      # 15 pages, no INE/departmental/NDVI/two-factor DFM
make paper-v2      # 21 pages, full Track A–H deliverable
```

---

## Credentials

`.env` is NEVER committed. Copy `.env.example` and fill in as needed.

| Variable | Required for | Status |
|---|---|---|
| `GCP_PROJECT_ID`, `GCS_BUCKET`, `GOOGLE_APPLICATION_CREDENTIALS` | Earth Engine + GCS exports | ✅ **Configured** — reusing `haiti-poverty-monitoring` + `haiti-sae-2026` + `/Users/whl/secrets/haiti-sae-runner.json` from whl.Haiti |
| `EOG_TOKEN` | VIIRS Nightfire (VNF) Chaco flare data | ⏳ **Awaiting EOG approval** — license request submitted 2026-04-24; token refresh via `make refresh-eog` once issued |
| `EOG_USER`, `EOG_PASS` | Auto-refresh EOG_TOKEN (60-day cadence) | Set once EOG account activated |
| `GH_PAT` | Push to `wernerhl/bolivia-satellite-proxy` | ✅ gh CLI authenticated |

The `.env.example` template and the `src/00_fetch/refresh_eog_token.py` helper
document the full setup.

---

## Data sources — exact webpages

Every download URL that produced the CSVs in `data/official/`. All sources are
public; no authentication needed except where noted.

### INE (Instituto Nacional de Estadística)

- **PIB trimestral (quarterly GDP, 1990 base)** —
  <https://www.ine.gob.bo/index.php/estadisticas-economicas/pib-y-cuentas-nacionales/producto-interno-bruto-trimestral/producto-interno-bruto-trimestral-intro/>
  → 22 xlsx cuadros + 1 methodology PDF on the INE Nimbus share.
  Parser: `src/00_fetch/parse_ine_pib.py`.

- **PIB departamental (annual departmental GDP, 1988–2016, 1990 base)** —
  <https://www.ine.gob.bo/index.php/estadisticas-economicas/pib-y-cuentas-nacionales/producto-interno-bruto-departamental/producto-interno-bruto-departamental/>
  → 10 xlsx (9 departments + national summary) on the INE Nimbus share.
  Parser: `src/00_fetch/parse_ine_dep_gdp.py`.

- **Imports microdata (Aduana)** —
  <https://www.ine.gob.bo/index.php/comercio/importaciones/>
  → 3 annual xlsx (2024, 2025, 2026 YTD) with customs-declaration detail.
  Parser: `src/00_fetch/parse_aduana_imports.py`.

### World Bank

- **WDI annual indicators for Bolivia (real GDP, reserves, imports, petroleum
  rents, fuel exports %)** — public JSON API:
  ```
  https://api.worldbank.org/v2/country/BOL/indicator/{INDICATOR}?format=json&date=2000:2026&per_page=100
  ```
  Fetcher: `src/00_fetch/fetch_worldbank.py`.

- **Global Gas Flaring Reduction (GGFR) estimates, 2012–2024** —
  Direct xlsx endpoints under
  <https://www.worldbank.org/en/programs/gasflaringreduction/global-flaring-data>:
  - Country-level: `https://thedocs.worldbank.org/en/doc/bd2432bbb0e514986f382f61b14b2608-0400072025/related/Flare-volume-and-intensity-estimates-2012-2024.xlsx`
  - Location-level: `https://thedocs.worldbank.org/en/doc/bd2432bbb0e514986f382f61b14b2608-0400072025/related/2012-2024-Flare-Volume-Estimates-by-individual-Flare-Location.xlsx`

  Fetcher: `src/00_fetch/fetch_wb_ggfr.py`.

### YPFB (Yacimientos Petrolíferos Fiscales Bolivianos)

- **Gas production indicators, published as JPG charts** —
  <https://www.ypfb.gob.bo/Gas_natural>
  → `/sites/default/files/2025-11/diapositiva{3,4_0,5,6,7,8}.jpg`.

  Chart 3 (annual gas production, 2006–2025) has numeric labels printed on
  bars; those values were read directly into
  `data/official/ypfb_gas_production_annual.csv`. For unlabeled charts, the
  OpenCV-based `src/00_fetch/chart_extractor.py` can digitize bar heights
  and line traces given manual axis calibration (framework scaffolded; not
  run against the other slides yet).

### Binance P2P

- **Parallel BOB/USD rate** —
  POST `https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search`
  with JSON body filtering fiat=BOB, asset=USDT, tradeType=BUY.
  Fetcher: `src/00_fetch/fetch_binance_p2p.py`.

### Satellite data via Google Earth Engine

All via the Haiti service account (`GOOGLE_APPLICATION_CREDENTIALS`).

- **VIIRS DNB monthly SOL** — `NASA/VIIRS/002/VNP46A2` (daily, BRDF+lunar
  gap-filled; rolled to monthly means) with `NOAA/VIIRS/DNB/MONTHLY_V1/VCMSLCFG`
  fallback when primary has <10 daily images in a month.
- **Sentinel-5P TROPOMI NO₂** — `COPERNICUS/S5P/OFFL/L3_NO2`,
  server-side monthly aggregation.
- **Sentinel-2 NDVI** — `COPERNICUS/S2_SR_HARMONIZED` primary (2017-03+),
  `LANDSAT/LC08/C02/T1_L2` fallback for the 2013–2017 baseline, with the
  Roy et al. (2016) cross-sensor NDVI harmonization. WorldCover 2021 crop
  mask (class 40) applied upstream of the reduction.

### Still awaiting data

| Series | Source | Blocker |
|---|---|---|
| VIIRS Nightfire (Chaco, monthly) | <https://eogdata.mines.edu/products/vnf/> | EOG license approval in progress |
| CNDC electricity (monthly SIN) | <https://www.cndc.bo/php/dload.php?f=gen_dia_MMYY.xlsx&d=estmes> | `www.cndc.bo` unresponsive from my network (status 0) |
| IBCH cement dispatches | <https://www.ibch.com/> | No public download path from the homepage; may require Fundación Milenio republication |
| SIN tax collections | <https://www.impuestos.gob.bo/> | Not yet scraped |
| YPFB field-month production | YPFB Boletín Estadístico | Published as PDF-per-month; OCR pipeline not yet built |

---

## Build

```bash
# One-time setup
cp .env.example .env          # fill EOG_TOKEN when approved
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# End-to-end (fetch → process → anomaly → index → econometrics → paper-v2)
make all

# Subsets
make fetch                    # all four satellite streams + WB + Binance + official
make process                  # VNF flare attribution, VIIRS→dept aggregation
make anomaly                  # per-stream anomalies
make index                    # CI + benchmark + alerts + DFM export
make econometrics             # elasticities, two-factor DFM, BBQ, Markov, manipulation tests
make paper-v1                 # compile paper/v1/fires_lights_smog.pdf
make paper-v2                 # regen figures + tables, compile paper/v2/fires_lights_smog.pdf
make test-scaffold            # pytest smoke tests
```

---

## Key results currently in `paper/v2/fires_lights_smog.pdf`

- Four satellite streams producing a monthly coincident indicator covering
  approximately 78 percent of GDP.
- Two-factor DFM (urban + extractive) with GDP-share weights 0.71 / 0.29,
  n = 169 monthly observations; LMST weighted-composite fallback when any
  block fails.
- VIIRS-to-GDP elasticity on Bolivian departmental panel:
  β = +0.128 (HAC SE 0.055, p = 0.02, n = 36, 9 depts × 4 years).
- BBQ troughs at 2019-01 and 2024-01; Hamilton Markov-switching identifies
  27 recession months.
- Test 1 sectoral triangulation: corr(WB-GGFR flaring, YPFB) = 0.064,
  corr(YPFB, INE hydrocarbon VA) = 0.993 → **"dry gas flaring not
  volumetric, no manipulation signal"** — exactly the Track C warning
  materializing in the data.
- Tests 2 and 3 correctly marked TBD pending post-Nov-2025 IGAE data and
  a longer external-forecaster annual panel.

---

## Agent instructions

Kept local under `agent_prompts/`, excluded from Git.

- `agent_prompts/bolivia_satellite_proxy_agent_prompt.md` — original
  pipeline spec (three-stream version, pre-revision).
- `agent_prompts/fires_lights_smog_revision_agent_prompt.md` — Track A–G
  revision brief.
- `agent_prompts/fires_lights_smog_revision_agent_promptv2.md` —
  adds Track H (figures + tables).

---

## License and disclaimer

All opinions are the author's own and do not reflect the views of any
affiliated institution. All remaining errors are mine.
