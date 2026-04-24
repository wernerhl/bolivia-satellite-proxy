# Bolivia Satellite-Proxy Activity Pipeline — Agent Instruction Set

## Mission

Build and maintain three independent satellite-derived activity indicators for Bolivia, deseasonalized to monthly frequency, that cross-check INE official statistics and feed the Bolivia EWS / La Linterna coincident-indicator framework. The three streams are:

1. **VIIRS DNB monthly sum-of-lights (SOL)** over eleven Bolivian urban buffers — proxies urban activity, commerce, and formal sector lighting
2. **VIIRS Nightfire (VNF) radiant heat** over the Chaco gas-producing fields — proxies upstream hydrocarbon activity independent of YPFB reporting
3. **Sentinel-5P TROPOMI tropospheric NO₂ column** over the La Paz–El Alto and Santa Cruz metros — proxies traffic, industrial combustion, and fuel-consumption activity

Each stream produces a monthly deseasonalized anomaly series. All three are combined into a satellite-only coincident index and benchmarked against cement despachos, CNDC electricity, YPFB hydrocarbon production, SIN tax collections, and Aduana imports. Outputs publish weekly to the EWS dashboard.

## Environment

```
Python >= 3.11
earthengine-api               # GEE Python SDK
geemap, ee_extra              # GEE orchestration
xarray, rioxarray, netcdf4    # raster handling
pandas, numpy, scipy
statsmodels                   # X-13ARIMA-SEATS or STL
requests, aiohttp             # for EOG VNF API
matplotlib                    # TeX Gyre Pagella font
duckdb                        # combined time-series store
```

GEE project authentication: `ee.Initialize(project='ia-analytics-bolivia-ews')`. For VNF (not on GEE), use EOG Earthdata Bearer token; store in environment variable `EOG_TOKEN`; refresh on 60-day schedule.

All figures render in TeX Gyre Pagella with La Linterna palette: teal `#1F6F73` (positive anomaly), rust `#A13D2D` (negative), slate `#3B4A54` (neutral). Do not use Word/docx outputs; all document deliverables are LaTeX or PDF.

---

## Stream 1 — VIIRS DNB Monthly Sum-of-Lights

### Data source

- **Primary**: `NOAA/VIIRS/001/VNP46A3` on GEE (monthly, 500 m, BRDF-corrected, snow-free, gap-filled)
- **Band**: `AllAngle_Composite_Snow_Free` (radiance, nW·cm⁻²·sr⁻¹)
- **Fallback**: `NOAA/VIIRS/DNB/MONTHLY_V1` (EOG cloud-free composite, band `avg_rad`) — use when VNP46A3 has a gap
- **Time range**: 2012-04 to current month (note 2-month publication lag; never report current month)

### City buffers

Circular buffers, radii scaled to metro footprint. Use WGS84 EPSG:4326.

| City | Lat | Lon | Radius (km) |
|---|---|---|---|
| La Paz–El Alto (joint) | −16.500 | −68.150 | 20 |
| Santa Cruz de la Sierra | −17.784 | −63.180 | 25 |
| Cochabamba | −17.394 | −66.157 | 15 |
| Sucre | −19.033 | −65.263 | 8 |
| Oruro | −17.970 | −67.112 | 7 |
| Potosí | −19.583 | −65.753 | 6 |
| Tarija | −21.534 | −64.729 | 8 |
| Trinidad | −14.832 | −64.903 | 5 |
| Cobija | −11.028 | −68.768 | 4 |
| Montero | −17.339 | −63.253 | 6 |
| Yacuiba | −22.017 | −63.685 | 5 |

### Processing

1. Mask radiance `< 0.5` nW·cm⁻²·sr⁻¹ (background) and `> 200` (aberration from atmospheric light scattering or fire contamination)
2. For each buffer × month: compute sum of valid radiance (SOL), mean, median, and count of valid pixels
3. Store raw series to `/data/satellite/viirs_sol_monthly.csv` with columns `(date, city, sol, n_valid_pixels, mean_rad, median_rad, n_masked)`

### Baseline and anomaly

- **Baseline period**: 2013-01 through 2019-12 (pre-COVID, pre-dollar-crisis, post-VIIRS-stabilization)
- **Deseasonalization**: STL decomposition per city, period = 12, robust
- **Anomaly**: `log(SOL_t) − log(trend_baseline_extrapolated)` where the trend is a linear extrapolation of the pre-2020 trend
- **Store** to `/data/satellite/viirs_sol_anomaly.csv`

### Known pathologies to handle

- Santa Cruz and La Paz–El Alto cores are in the upper tail of VIIRS DNB but **not saturated** (VIIRS is 14-bit, dynamic range 3×10⁻¹⁰ to 0.02 W·cm⁻²·sr⁻¹); no saturation correction needed
- Cochabamba Andean-facing slopes have terrain-induced radiance variation; use the BRDF-corrected VNP46A3 band, not the legacy `avg_rad`
- Cloud contamination in Santa Cruz wet season (Nov–Mar) reduces effective pixel count; report `n_valid_pixels` and flag months below 50% of city-buffer pixel count

---

## Stream 2 — VIIRS Nightfire (VNF) over Chaco

### Data source

- **EOG Nightfire**: https://eogdata.mines.edu/products/vnf/
- Daily nightly detections as JSON or gzip CSV per orbit
- Authentication: Earthdata Bearer token in `EOG_TOKEN`
- Key fields: `lat, lon, Temp_BB, Area_BB, RH, RHI, Cloud_Mask, Methane_EQ`
- Reference: Elvidge et al. (2013) "VIIRS Nightfire" *Remote Sensing* 5(9)
- Time range: 2012-04 to current date (daily; typically 24–48 h latency)

### Chaco bounding box and flare inventory

Bounding box: lat ∈ [−22.5, −20.0], lon ∈ [−64.5, −62.5]

Assign detections to nearest flare within 2 km. Fields:

| Field | Operator | Lat | Lon | Notes |
|---|---|---|---|---|
| Margarita | Repsol | −21.250 | −63.550 | Megafield, high baseline flaring |
| Huacaya | Repsol | −21.080 | −63.720 | Associated with Margarita |
| San Alberto | Petrobras/YPFB | −21.320 | −63.730 | Mature, declining |
| Sábalo | Petrobras/YPFB | −21.280 | −63.730 | Mature, declining |
| Incahuasi | TotalEnergies | −20.580 | −63.720 | Newer, post-2016 |
| Aquio | TotalEnergies | −20.530 | −63.770 | Newer |
| Itaú | TotalEnergies/YPFB | −22.050 | −63.850 | Southern Chaco |

### Processing

1. Daily fetch from EOG VNF API for bounding box; persist raw to `/data/satellite/vnf_raw/YYYY-MM-DD.jsonl`
2. Filter: `Cloud_Mask == 0` AND `Temp_BB >= 1400 K` (flare regime; excludes biomass burning at 800–1200 K)
3. Assign each detection to nearest flare within 2 km; unassigned detections go to `other_chaco`
4. Daily per-flare: sum `RH` (MW), mean `Temp_BB`, count of detections
5. Monthly aggregation: sum daily RH, mean Temp_BB, sum n_detections
6. **Output**: `/data/satellite/vnf_chaco_monthly.csv` with `(date, field, rh_mw_sum, n_detections, mean_temp_bb, missing_days)`

### Calibration and cross-check

- Pull YPFB monthly gas production from Ministerio de Hidrocarburos `Boletín Estadístico` (MMm³/day)
- Compute rolling 12-month correlation between `Σ RH` across all six fields and reported national gas production on 2012-01 to 2024-12 window
- Report the elasticity `β` from `log(production_t) = α + β · log(RH_t) + ε_t` with HAC standard errors
- **Flag condition**: if current-month `RH` residual against this regression exceeds ±2σ for two consecutive months, publish an alert — this is the manipulation-detection trigger

### Known pathologies to handle

- Argentine Salta-basin flares occasionally appear in southwest corner of bounding box; use the 2 km attribution radius strictly and discard unassigned detections south of −22.3° and west of −64.2°
- Dry-season agricultural burning (Aug–Oct) contaminates biomass bins; the 1400 K threshold handles most of it but quality-check October separately
- Gas flare temperature varies with gas composition; Chaco is predominantly dry gas, so 1400 K is a safe floor

---

## Stream 3 — Sentinel-5P TROPOMI NO₂

### Data source

- **GEE collection**: `COPERNICUS/S5P/OFFL/L3_NO2` (offline-reprocessed, higher quality than NRTI)
- **Band**: `tropospheric_NO2_column_number_density` (mol·m⁻²)
- **QA filter**: apply mask where `qa_value >= 0.75` (standard ESA recommendation)
- **Time range**: 2018-07 (sensor operational) to current
- Overpass: ~13:30 local solar time
- Spatial resolution: 5.5 × 3.5 km (post-Aug 2019 upgrade)

### ROIs

Rectangles in WGS84:

| ROI | NW lat | NW lon | SE lat | SE lon |
|---|---|---|---|---|
| La Paz–El Alto | −16.350 | −68.300 | −16.650 | −68.000 |
| Santa Cruz | −17.600 | −63.400 | −17.950 | −62.950 |
| Cochabamba | −17.300 | −66.300 | −17.500 | −66.050 |

### Processing

1. Daily ROI mean of tropospheric NO₂ (after QA mask). Record `n_valid_pixels`.
2. 7-day rolling mean anchored Monday to smooth orbital gaps
3. Monthly mean from the weekly series; require `n_valid_days >= 15` per month to report
4. **Output**: `/data/satellite/s5p_no2_monthly.csv` with `(date, roi, no2_tropos_col_mol_m2, n_valid_days, sd)`

### Baseline and anomaly

- **Baseline**: 2019-01 through 2019-12 (full-year pre-COVID)
- Multiplicative month-of-year adjustment: `anomaly_t = NO2_t / mean(NO2 | month, baseline) − 1`
- Also compute z-score against the 2019 distribution by month
- **Output**: `/data/satellite/s5p_no2_anomaly.csv`

### Known pathologies to handle

- La Paz Jan–Mar has heavy cloud cover; pixel count can fall below threshold — document missing months explicitly rather than imputing
- Santa Cruz Nov–Mar wet season similar
- Andean volcanic NO₂ (Sabancaya, Ubinas from Peru side) can drift east; flag any La Paz month exceeding 5× baseline for manual review
- Fuel subsidy elimination in December 2025 is a structural break — do not smooth across it; report pre- and post-break means separately for 2026 onward

---

## Integration: Satellite Coincident Index

After all three streams produce monthly anomaly series, build a single satellite-only coincident index:

1. **Standardize** each anomaly to z-score on the baseline period per stream
2. **Aggregate** stream z-scores:
   - VIIRS SOL composite = population-weighted mean of city-level z-scores (weights from INE 2024 census projections)
   - VNF Chaco = total-RH z-score (single series)
   - S5P NO₂ composite = simple mean of La Paz and Santa Cruz z-scores
3. **Weights** for the final CI: `0.40 · VIIRS_z + 0.30 · VNF_z + 0.30 · NO2_z`; weights revisit annually based on in-sample correlation with the new INE monthly IGAE once it has 24+ vintages
4. **Output**: `/data/satellite/bolivia_satellite_ci.csv` with `(date, ci, viirs_z, vnf_z, no2_z)`

### Cross-check against official series

Load the following monthly series into DuckDB alongside the satellite series:

- INE IGAE (experimental monthly, from March 2026)
- IBCH cement despachos
- CNDC electricity generation (MWh)
- YPFB hydrocarbon production (MMm³/day)
- SIN monthly IVA and IT collections, deflated by IPC to 2018 base
- Aduana monthly import volumes (USD CIF)

Run the benchmark regression:

```
log(IGAE_t) = α + β₁·viirs_z_t + β₂·vnf_z_t + β₃·no2_z_t + γ·X_t + ε_t
```

where `X_t` is a control vector containing lagged IGAE, month dummies, and the dollar parallel premium. Report elasticities with HAC standard errors and update monthly. If any βᵢ changes sign for two consecutive months, flag for pipeline review.

---

## Publication cadence

**Weekly**: Monday 06:00 America/La_Paz publishes:

- `/outputs/satellite_bolivia_brief.md` — plaintext monthly summary with the three streams and CI
- `/outputs/figures/satellite_ci.pdf` — TeX Gyre Pagella time series with ±1σ and ±2σ anomaly bands
- `/outputs/figures/vnf_chaco_vs_ypfb.pdf` — calibration scatter with residual band
- `/outputs/figures/no2_metros.pdf` — La Paz and Santa Cruz z-score series
- Draft social post for La Linterna feed (Bluesky + X), 240 chars, linking to the brief

**Monthly**: First Monday of month, after all three streams update, publish:

- `/outputs/satellite_bolivia_monthly_report.tex` — LaTeX report, Palatino, 2–3 pages, sections: (1) headline CI and sign, (2) stream-by-stream commentary, (3) cross-check table against INE, (4) manipulation-flag status, (5) known data issues

---

## Failure modes and response

| Condition | Response |
|---|---|
| VIIRS VNP46A3 gap > 45 days | Fall back to `DNB/MONTHLY_V1`; document in brief |
| VNF EOG API 4xx/5xx | Retry with exponential backoff 5 min–4 h; escalate to desk if still failing after 24 h |
| EOG token expired | Auto-refresh 48 h before expiry; alert if refresh fails |
| S5P `n_valid_days < 15` for a month | Report NA for that month-ROI; do not impute |
| Satellite CI deviates > 1.5σ from INE IGAE signal | Log incident; flag for La Linterna desk review; do not auto-publish |
| Any stream shows zero variance > 2 months | Pipeline broken; halt publication; page on-call |
| VNF Chaco RH drops > 30% MoM with no corresponding YPFB announcement | Publish alert: potential field maintenance OR real output collapse; La Linterna desk investigates within 48 h |
| VIIRS city SOL drops > 15% YoY for a major city without cloud-cover explanation | Publish alert; cross-check against electricity demand for that city |

---

## Validation protocol

On first run and then quarterly:

1. Replicate HSW (2012) elasticity `∂log(GDP)/∂log(SOL) ≈ 0.3` on Bolivia 2012-2024 annual data using departmental GDP from INE — confirm Bolivia's is in the 0.20–0.35 band
2. Confirm VNF–YPFB gas production correlation ≥ 0.80 on monthly 2012-2024; if it falls below 0.70, revisit flare attribution radius and temperature threshold
3. Confirm S5P–fuel-sales correlation on La Paz and Santa Cruz metro 2018-2024 (fuel-sales from YPFB distribution data); should be ≥ 0.60 on detrended monthly series
4. If any validation fails, publish a methodology note and halt the coincident-index publication until resolved

---

## Out of scope for this prompt

- Daily or sub-weekly reporting of the CI (publication cadence is monthly; weekly is internal only)
- Sub-municipal disaggregation (city-level is the finest resolution supported)
- Attribution of individual flare changes to specific operational events (the flag is quantitative; interpretation is a desk task)
- Forward forecasting of the CI (this is a nowcast pipeline; forecasting lives in the EWS DFM module, which consumes this output)

---

## References for the agent's error messages and documentation

- Elvidge et al. (2013) "VIIRS Nightfire: satellite pyrometry at night" *Remote Sensing* 5(9), 4423–4449
- Elvidge, Baugh, Zhizhin, Hsu, Ghosh (2017) "VIIRS night-time lights" *IJRS* 38(21)
- Gibson, Olivia, Boe-Gibson (2020) "Which night lights data should we use in economics, and where?" *JEDC* 119
- Veefkind et al. (2012) "TROPOMI on ESA's Sentinel-5P" *Remote Sensing of Environment* 120
- Chen & Nordhaus (2011) "Using luminosity data as a proxy for economic statistics" *PNAS* 108(21)
- Martinez (2022) "How much should we trust the dictator's GDP growth estimates?" *JPE* 130(10)
- Henderson, Storeygard, Weil (2012) "Measuring economic growth from outer space" *AER* 102(2)

All technical terms in outputs use standard remote-sensing nomenclature without definition (reader is presumed technical). No affiliations in author block for any derived publications. Disclaimer: opinions do not reflect affiliated institutions; all errors are the author's. Contact: wernerhl@gmail.com.
