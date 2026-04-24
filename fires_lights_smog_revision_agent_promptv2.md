# "Fires, Lights, and Smog" — Paper Revision Agent Instructions

**Target paper.** Hernani-Limarino, *Fires, Lights, and Smog: Reading Bolivia's Recession from Space*, current version `fires_lights_smog.tex` (12 pages, results empty, compiles clean).

**Goal.** Take the paper from empty-stub-with-critique to submission-ready v2 draft, resolving in priority order the four tiers of problems identified in the internal critique. All tracks run in LaTeX, Palatino (mathpazo + TeX Gyre Pagella), no affiliations in author block, wernerhl@gmail.com as contact, Acemoglu register (claim-lead paragraphs, causal density 30–40%, zero evaluative adjectives, specificity with numbers), scorecard target ≥42/50.

Work ordered by severity. Do not advance to the next tier until the current tier's deliverables compile cleanly and are reviewed.

---

## TRACK A — Close the agricultural blind spot (Tier 1, existential)

### Problem

Agriculture was +12.19 percent interannual in Q4 2025 and is roughly 14 percent of Bolivian GDP. None of VIIRS DNB, VNF, or TROPOMI NO₂ sees it. A satellite-implied contraction deeper than INE's is observationally equivalent between (a) INE smoothing and (b) satellite blindness to the one expanding sector. The paper cannot claim falsification until this is closed.

### Deliverable

Add Sentinel-2 NDVI as a fourth satellite stream, at monthly frequency, over five cropland zones. Update the paper's title, abstract, and §3 to reflect four streams, not three. Rename the paper internally as "Fires, Lights, Smog, and Green" (keep the public title *Fires, Lights, and Smog* — the "green" is technical, not marketing).

### Data

Source: Copernicus Sentinel-2 Level-2A surface reflectance via Google Earth Engine collection `COPERNICUS/S2_SR_HARMONIZED`. Operational 2017-03 onward; splice with Landsat-8 `LANDSAT/LC08/C02/T1_L2` back to 2013 for the baseline period using the Roy et al. (2016) cross-sensor harmonization coefficients.

NDVI = (B8 − B4) / (B8 + B4). Cloud mask via the SCL band (exclude classes 3, 8, 9, 10, 11). Monthly composite = median of valid pixel observations per month, aggregated to zone-monthly mean NDVI.

### Cropland zones

Five zones covering >90 percent of Bolivian agricultural value added:

| Zone | Bounding polygon or centroid+radius | Dominant crop | INE GVA weight |
|---|---|---|---|
| Santa Cruz soy belt | Centroid (−17.15, −62.50), radius 180 km, crop-masked | Soy, maize, sorghum | ~45% |
| Beni cattle and rice | Centroid (−14.50, −65.00), radius 150 km | Rice, cattle pasture | ~15% |
| Tarija Valle Central | Centroid (−21.55, −64.75), radius 40 km | Grapes, stone fruit, maize | ~5% |
| Chaco periphery | Centroid (−20.50, −63.00), radius 120 km | Soy, sorghum | ~10% |
| Altiplano tubers and quinoa | Centroid (−19.00, −67.00), radius 100 km | Potato, quinoa, barley | ~10% |

Coordinates above are indicative; refine against the ESA WorldCover 2021 cropland layer before first run.

### Processing

1. For each zone × month, extract monthly median NDVI over crop-masked pixels only (WorldCover class 40).
2. Compute anomaly = NDVI_zt − mean(NDVI_z | month-of-year, 2013–2019) / σ_z,month.
3. Weight zones by INE departmental agricultural GVA (as indicated above, refine from 2024 national accounts).
4. Output `/data/satellite/s2_ndvi_monthly.csv` with `(date, zone, ndvi_mean, n_valid_pixels, anomaly_z)`.

### Integration

- Add as fourth stream in §3.1 with a new subsection 3.1.4. Renumber Tables 1–3 to include an NDVI zone table.
- Add fourth elasticity equation in §4.1: `Δlog(AgGVA_d,t) = α_d + β_NDVI · Δlog(NDVI_d,t) + γ_t + ε_d,t`, panel of 9 departments × annual 2013–2024.
- Weight in composite: revise from 0.40 VIIRS / 0.30 VNF / 0.30 NO₂ to 0.30 VIIRS / 0.25 VNF / 0.20 NO₂ / 0.25 NDVI.
- Add to §3 a single paragraph acknowledging services (the other INE-invisible sector satellites miss) and scoping the composite to "goods-producing and tradeable-services GVA, covering ~78 percent of GDP."

### Literature to add

Gao (1996) NDVI as vegetation activity proxy; Donohue et al. (2008) NDVI–GPP relationship; Johnson et al. (2014) NDVI for crop-yield forecasting in the US. Economics-register: Donaldson & Storeygard (2016) *JEP* "The View from Above" as general cite for agricultural remote sensing in economics.

### Acceptance criterion

Section 3 runs four streams. Paper claims "the composite captures ~78 percent of GDP; the remaining ~22 percent (services, largely non-tradeable) is discussed as a known blind spot in §6." Agricultural expansion stops being a referee-killable omission.

---

## TRACK B — Rebuild the manipulation-detection test (Tier 1, existential)

### Problem

Test 1 as written (pre/post trust break on 2006–2014 vs. 2020–2024) is jointly identified off manipulation, COVID, the parallel-rate emergence, and post-pandemic supply disruption. With n=1 country, this is not credible identification. Martinez (2022) got identification from 184 countries of regime variation; Bolivia supplies one.

### Deliverable

Rewrite §4.3 around three tests with genuine single-country identification.

### Test 1 (new) — November-2025 INE leadership discontinuity

INE's director changed in November 2025 with the Paz inauguration. Pre-Nov-2025 data were produced by MAS-era leadership; post-Nov-2025 by Paz-appointed leadership. Hydrocarbon fundamentals, reserves trajectory, and parallel-rate premium are continuous across this break. Any sharp change in the satellite–GDP elasticity at the break is attributable to statistical production, not fundamentals.

Specification:
```
Δlog(IGAE_t) = α + β · Δlog(CI_t) + δ · D^post-Nov25_t + γ · [Δlog(CI_t) · D^post-Nov25_t] + X_t · ψ + ε_t
```
where CI_t is the satellite composite, D^post-Nov25 is the leadership-change indicator, X_t is a control vector (parallel premium, reserve change, seasonal dummies, precipitation anomaly). Test H₀: γ = 0 against H₁: γ < 0 (pre-Nov INE showed less GDP movement per unit of satellite activity, consistent with upward smoothing during the 2024–Oct2025 acute crisis period).

Bandwidth: 12 months each side minimum, 24 months preferred. Bolivia's monthly IGAE starts March 2026, so the post-break sample has ≤5 observations as of submission. Explicitly flag this as preliminary; update at R&R. Use quarterly GDP if IGAE panel is too short.

### Test 2 — Residual falsification against external forecasters

Regress the satellite composite on INE quarterly GDP growth and separately on external forecasters (IMF, World Bank, Oxford, S&P). Report residuals. The null is that INE residuals and external residuals have the same sign and magnitude. Rejection in favor of systematically more negative INE residuals during 2024–2025 is evidence of INE smoothing relative to the external consensus, which is itself anchored in the same satellite and alt-data universe.

### Test 3 — Sectoral triangulation across INE, YPFB, and VNF

Unchanged from current draft. This is the paper's cleanest contribution. Frame it as the primary test and lead §4.3 with it.

### Reorder §4.3

Lead with the sectoral triangulation (current Test 3, best single-country identification). Follow with the discontinuity test (new Test 1). Close with the external-forecaster residual (new Test 2). The narrative becomes: the physical evidence (VNF flaring) is the bedrock; the statistical evidence (discontinuity) is the middle layer; the external-consensus evidence (forecasters) is the ceiling. A rejection at all three layers is the strong claim; rejection at the physical layer alone is the weak-but-publishable claim.

### Acceptance criterion

Each test has an identifying assumption stated in one sentence. Each test has a null, an alternative, and a procedure. No test is jointly identified off COVID, the parallel rate, and leadership changes simultaneously.

---

## TRACK C — De-risk the VNF-to-production mapping (Tier 1)

### Problem

Do et al. (2018) recovered near-unity VNF-to-production elasticities for ISIS-controlled oil fields, where flaring scales with oil production through associated-gas venting. Bolivian Chaco fields are predominantly dry gas; flaring is operational (compressor downtime, emergency vents, start-up) rather than volumetric. The Elvidge et al. (2016) calibration is flare-to-BCM-flared, not flare-to-production.

### Deliverable

Estimate the Bolivia-specific VNF-to-production relationship on pre-crisis data before writing any §5 text that treats the elasticity as near unity. Decide the framing based on the estimate.

### Procedure

1. Pull YPFB field-level monthly production 2012–2022 from the Boletín Estadístico de Hidrocarburos (monthly field breakout appears as an annex). Manual download; archive raw PDFs to `/data/official/ypfb/raw/`.
2. For each of the six fields in Table 2, compute monthly Σ RH from VNF 2012–2022 using the procedure in §3.1.2 of the current draft.
3. Estimate equation (2) as specified. Report β_VNF with HAC standard errors for each field and pooled.
4. Also estimate a level-on-level regression `log(GasProd_f,t) = α_f + β · log(ΣRH_f,t) + γ_f · t + ε_f,t` with field fixed effects and field-specific trends, to allow for the structural difference between dry-gas operational flaring and volumetric flaring.

### Decision tree on the estimate

- **If pooled β_VNF ∈ [0.7, 1.3] with tight SEs and pre-crisis R² > 0.6**: keep current framing. VNF is a volumetric proxy. Cite Do et al. as directly applicable.
- **If pooled β_VNF ∈ [0.3, 0.7] or R² ∈ [0.3, 0.6]**: reframe VNF as a "capacity-utilization and operational-status proxy." Modify §3.1.2 and §4.1 to state that VNF captures the operational margin (when fields run hot vs. cold) but not absolute volumes. Cross-check against NO₂ for volumetric inference. Remove the Do et al. near-unity parallel; replace with a passage explicitly distinguishing dry-gas from associated-gas regimes.
- **If R² < 0.3**: VNF is not a production proxy in this application. Keep it in the paper as a *hydrocarbon-sector-activity indicator* (captures whether the fields are running, captures operator-level decisions around maintenance and venting), not as a production measure. Test 3 in §4.3 becomes a test of whether YPFB reports production that is inconsistent with observed operational intensity, which is still publishable but requires reframing.

### Acceptance criterion

§3.1.2 and §4.1 accurately describe what VNF measures in the Bolivian context. No text survives that implies "VNF radiant heat ≈ gas production" without the underlying calibration having been estimated and reported.

---

## TRACK D — Right-size the factor model (Tier 2)

### Problem

Seven-series DFM on three highly correlated urban satellite signals and four highly correlated official indicators collapses onto an "urban common variance" factor and is sold beyond what the panel dimension supports.

### Deliverable

Replace the single-factor DFM with a two-factor specification separating urban and extractive activity. Alternatively, if a referee flags even two factors as overfit, fall back to a pre-specified weighted average following Lewis-Mertens-Stock.

### Two-factor specification

- f_urban_t: loads VIIRS DNB composite, TROPOMI NO₂ composite, cement, electricity, real IVA, imports
- f_extractive_t: loads VNF Chaco, YPFB gas production, hydrocarbon-sector electricity demand
- Agricultural NDVI (from Track A) as a third factor OR as a separate conditioning variable; decide after estimation.

State-space form parallels the current §4.2 equations (4)–(7) with i ∈ {1, ..., 6} for f_urban and i ∈ {1, ..., 3} for f_extractive, with the Mariano–Murasawa aggregation identity applied to a weighted sum of the two factors matching GDP sectoral shares.

### Fallback specification

If estimation is unstable (EM algorithm fails to converge or factors are not well-separated), drop the state-space framework entirely and use:

```
CI_t = w_urban · z(urban composite) + w_extractive · z(extractive composite) + w_ag · z(NDVI composite)
```

with weights pre-specified from GDP sectoral shares and no estimation beyond standardization. Cite Lewis-Mertens-Stock (2022) as precedent. This is the honest spec if the DFM is overparameterized.

### Acceptance criterion

§4.2 delivers a CI that either (i) has two or three interpretable factors with loadings that match the sectoral priors, or (ii) is explicitly a pre-specified weighted average with no estimation. No one-factor DFM on seven correlated series survives the revision.

---

## TRACK E — Frame the paper decisively (Tier 2)

### Problem

Abstract frames as falsification. §4.3 has three tests of which only one is Martinez-style. Results could show small residuals, in which case the falsification frame fails and the paper has no backup story.

### Deliverable

Commit to one of two framings. Revise abstract, introduction, and §4.3 to match.

### Option 1 — Falsification frame (target: JPE, REStud, AEJ: Macro)

Keep abstract as written. Sharpen §4.3 Test 3 (sectoral triangulation) as the headline. Commit to the claim that *if* the satellite composite deviates substantially from INE, the paper argues that INE understates; *if* it does not, the paper has a null result on manipulation and a positive result on the coincident indicator as secondary contribution. Write §7 conditional on the headline result being positive.

### Option 2 — Coincident-indicator frame (target: JDE, JAE, IJF, Economic Modelling)

Rewrite abstract: "This paper constructs the first monthly coincident indicator of Bolivian real activity from four independent satellite streams… The indicator enables external cross-checks of INE's quarterly national accounts, which the paper demonstrates on the 2025–2026 recession." Move manipulation-detection to §5.5 as an application of the indicator rather than the paper's central purpose. This framing survives any empirical result.

### Recommendation

Option 2 is the safer target. Option 1 is the higher-ceiling target. Pick based on the tightness of the VNF calibration from Track C and the discontinuity-test results from Track B. If both come back strong, pursue Option 1; if either is weak, Option 2.

### Acceptance criterion

Abstract, introduction, §4, §7, and the planned §5 structure are internally consistent in framing. No passage survives that sells Option 1 language while the empirical design delivers Option 2.

---

## TRACK F — Tier 3 workflow and data archiving

### Problem

No APIs for INE, YPFB, IBCH, CNDC, SIN, Aduana. Manual downloads. This is a publication workflow problem, not a paper content problem, but the reproducibility claim in §1 requires it be solved.

### Deliverable

Two separable data systems.

### Paper dataset — frozen at submission

One clean pull of every series at submission date. Freeze as a Parquet dataset archived to Zenodo with DOI. Cite the DOI in the paper. The archive contains:

- `ine_gdp_quarterly_2017base.parquet`: constant-price quarterly GDP by sector, 2012Q1 to latest
- `ine_igae_monthly.parquet`: experimental IGAE, March 2026 onward
- `ine_dep_gdp_annual.parquet`: departmental GDP, 2013–2024
- `ypfb_field_production_monthly.parquet`: field-month gas production from BEH annexes
- `ibch_cement_monthly.parquet`: cement dispatches via Fundación Milenio republication
- `cndc_electricity_daily.parquet`: CNDC daily generation and demand
- `sin_recaudacion_monthly.parquet`: monthly IVA, IT, IUE, deflated
- `aduana_imports_monthly.parquet`: import volumes and values, with 45-day vintage lag
- Four satellite streams from Tracks A, current §3, and Track C
- BCB monthly monetary execution and weekly OMA

All raw PDFs and Excel exports archived under `/data/raw/` with date-stamped filenames. Processing scripts in `/scripts/` with deterministic output.

### Live dashboard — La Linterna / wernerhl.github.io/bolivia-ews

Monthly scraper pipeline feeding the Memoria archive. Documented cadence: satellite streams update 48–72 h after month end; INE and YPFB update 45–75 days after month end; full composite stable vintage at t−60 days. Every dashboard render stamps the vintage date. No backfill: historical vintages preserved so researchers can reconstruct what was known when.

### Acceptance criterion

Paper cites a Zenodo DOI for the frozen replication dataset. Dashboard has explicit vintage stamps and does not silently overwrite prior readings.

---

## TRACK G — Tier 4 one-hour fixes

Resolve in one pass, no analysis required:

1. Replace `[TBD: XX]` in §1 with "approximately 40 percent" (La Paz department ~25%, Santa Cruz department ~31%, minus periurban and rural components). Cite INE 2024 departmental GDP.
2. Fix equation numbering in Table 4. Equations are (1), (2), (3) — not (6), (7), (8).
3. Add citation to Bolívar & Cuba (2024) in §1 paragraph on contributions: "Bolívar and Cuba (2024) nowcast INE's own quarterly GDP using machine-learning methods; this paper's design is distinct — we cross-check INE from outside rather than forecast it from within."
4. Cut "four contributions" paragraph to two: (i) first published satellite triangulation for low-capacity statistical environments, (ii) first application to Bolivia 2025–2026 recession with new fourth stream.
5. Delete the paragraph "The stakes reach beyond Bolivia…" It is cheerleading. If countries beyond Bolivia are relevant, they belong in §7 conclusion as one sentence: "The framework applies directly to Venezuela, Angola, and post-2021 Afghanistan."
6. Remove the stub figures and tables in §5.2–§5.5 that render with placeholder output. Current compiled PDF shows BBQ turning points and Markov-switching parameters (μ_r=−0.98, 3 recession months) that were generated from noise, not data. These are dangerous if any reader misses the "DFM not yet fit" line. Replace with: `\tbdline{figure to be inserted after empirical estimation}` placeholders, rendered in the TBD box style.
7. In the knowledge_cutoff / current-events context: confirm all macro figures in §2 are current as of the submission date. The −1.58% 2025 GDP print, the $103 MM September 2025 reserve figure, the 2002→466 bps EMBI compression, and the CIN-SPNF January execution number are all current as of April 2026; if submitting later, re-verify.
8. Add `\usepackage{siunitx}` if not already; paper uses `\num{}` and `\numrange{}`.

### Acceptance criterion

Clean `pdflatex → bibtex → pdflatex → pdflatex` build with zero warnings beyond the standard microtype patch and hyperref-unicode-in-bookmark cosmetic messages.

---

## TRACK H — Figures and tables

### Problem

Current v1 has no real figures, two stub figures rendered from noise (dangerous), and three tables that are mostly placeholders. The revision needs a disciplined figure and table plan that earns every slot. Acemoglu-register papers carry four to six figures, each with a clear argumentative purpose. Decoration is penalized.

### Deliverable

Five main-text figures, four main-text tables, two appendix figures, two appendix tables. Each figure specified below with its argumentative purpose, exact content, and generation requirements. The agent generates all figures from the Parquet dataset frozen in Track F; tables are written in LaTeX `booktabs` format with numbers pulled from the same dataset.

### Standing conventions

All figures and tables follow these rules without exception.

- **Font.** TeX Gyre Pagella via matplotlib: `rcParams["font.family"] = "serif"; rcParams["font.serif"] = ["TeX Gyre Pagella"]; rcParams["text.usetex"] = True; rcParams["text.latex.preamble"] = r"\usepackage{mathpazo}"`. Test that TeX is available; if not, fall back to `"serif"` family without LaTeX rendering and flag in the figure-generation log.
- **Palette.** La Linterna colors only. Teal `#1F6F73` for positive anomalies or expansion regimes. Rust `#A13D2D` for negative anomalies or contraction regimes. Slate `#3B4A54` for neutral or reference series. Ochre `#C08A3E` for tertiary accents when a third color is required. No default matplotlib blue-orange. No viridis or other colormap defaults on categorical data; colormaps are acceptable only for continuous spatial overlays in Appendix Figure A1.
- **Line weights.** Primary series 1.2 pt. Comparison or benchmark series 0.8 pt. Axis spines 0.5 pt.
- **Gridlines.** Off by default. On scatter plots only, 0.3 pt major grid at `alpha=0.3`.
- **Legends.** Inside the plot area where space allows. No external legend boxes. No legend frame borders (`frameon=False`).
- **Annotations.** Vertical reference lines with short inline labels rotated 90° and sized at 7 pt. Recession bands as `axvspan` at `alpha=0.08`.
- **File format.** PDF via `savefig(path, dpi=400, bbox_inches="tight", pad_inches=0.05)`. No PNG in LaTeX. Each figure also rendered at 150 dpi PNG for dashboard use, stored separately under `/figures/png/`.
- **Size.** Single-panel figures 6.0 × 3.5 inches. Two-panel figures 6.0 × 5.5 inches stacked or 10.0 × 3.5 side-by-side (two-column, for appendix only). Four-panel 10.0 × 7.0 two-by-two.
- **Captions.** LaTeX captions terse, claim-forward, Acemoglu register. No "This figure shows…" preambles. Format: single claim sentence, followed by a source and notes sentence.

### Main-text figures

#### Figure 1 — Four satellite streams, raw monthly series, 2012–2026

**Purpose.** Establish that each stream carries independent signal and that the 2025–2026 contraction is visible on every panel. The reader should leave this figure convinced that four independent instruments agree.

**Layout.** Four stacked panels, shared x-axis, 6.0 × 7.0 inches. Top: VIIRS DNB sum-of-lights, population-weighted composite across 11 urban buffers, z-score against 2013–2019 baseline. Second: VNF radiant heat, Chaco aggregate across six fields, z-score. Third: TROPOMI NO₂, La Paz + Santa Cruz mean, z-score against 2019 seasonal baseline (note the 2018-07 start). Fourth: Sentinel-2 NDVI, five-zone weighted composite, z-score against 2013–2019 seasonal baseline (note the 2017-03 Sentinel-2 start; pre-2017 from Landsat-8 harmonization per Track A).

**Annotations.** Shaded vertical bands at INE-reported contractions (2020 Q2–Q3 COVID; 2024 Q1–2025 Q4 current). Vertical dashed line at Nov 2025 (Paz inauguration). Vertical dashed line at Dec 2025 (fuel subsidy elimination). Horizontal line at zero in each panel.

**Caption.** "Four independent satellite streams all show the 2025–2026 Bolivian contraction. Panels from top: urban nighttime radiance (VIIRS DNB, 11 metros), Chaco gas-flare radiant heat (VIIRS Nightfire, 6 fields), metropolitan NO₂ column (TROPOMI, La Paz–El Alto and Santa Cruz), and cropland NDVI (Sentinel-2, 5 zones). All series in z-scores against pre-crisis baselines; shaded bands mark INE-reported contractions; dashed lines mark the November 2025 government transition and December 2025 fuel-subsidy elimination."

#### Figure 2 — Bolivia elasticities versus the literature

**Purpose.** Show that the single-stream calibrations are in the expected range before any identification is asked to do work. The figure that survives a hostile referee on measurement.

**Layout.** Coefficient plot (forest plot), 6.0 × 4.0 inches. Four rows, one per stream. Each row shows two markers: Bolivia point estimate with 95% CI (filled rust circle with whiskers), literature benchmark with 95% CI or reported range (hollow slate diamond with whiskers). Vertical reference line at zero.

**Rows.**
- β_VIIRS (log GDP on log SOL, departmental panel) vs. HSW 2012 cross-country (≈0.3) and Beyer-Hu-Yao 2022 quarterly EMDE (1.36–1.81). Use two benchmarks on the same row, stacked.
- β_VNF (log gas production on log ΣRH, field panel) vs. Do et al. 2018 ISIS benchmark (≈1.0). Note whether the Bolivia estimate is consistent with the volumetric interpretation or with the operational-intensity interpretation (Track C decision tree).
- β_NO₂ (log fuel sales on log NO₂, metro panel) vs. Bauwens 2020 and Liu 2020 COVID-era ranges (0.2–0.5).
- β_NDVI (log agricultural GVA on log NDVI, departmental panel) vs. Johnson 2014 US corn-yield benchmark.

**Annotations.** Bolivia sample size `n` on the right of each row. Literature benchmark source cited in the caption.

**Caption.** "Bolivia-specific elasticities fall within the ranges established in the satellite-to-activity literature. Filled circles are Bolivia point estimates with 95% HAC-clustered confidence intervals; hollow diamonds are benchmarks from \citet{hsw2012}, \citet{beyer_hu_yao2022}, \citet{do_etal2018}, \citet{bauwens2020}, \citet{liu_china2020}, and Johnson et al. (2014)."

#### Figure 3 — The monthly composite and INE quarterly GDP

**Purpose.** The headline figure. This is the figure that travels in seminar slides, policy briefs, and the paper's elevator pitch.

**Layout.** Single panel, 6.5 × 4.0 inches. Left y-axis: satellite composite, z-score, monthly, 2012–2026, teal line (1.2 pt) with 68% shaded band (teal, `alpha=0.25`) and 95% shaded band (teal, `alpha=0.10`). Right y-axis: INE quarterly real GDP growth (year-on-year), stepped rust line (0.8 pt). Horizontal line at zero on both axes.

**Annotations.** Vertical dashed lines at (i) 2020-03 COVID onset, (ii) 2023-Q1 parallel-rate emergence, (iii) 2025-11 Paz inauguration and INE leadership change, (iv) 2025-12 fuel subsidy elimination. Labeled inline, 7 pt, rotated 90°. Horizontal dotted line at z = 0. Callout arrow pointing to the BCB's claimed Q4 2025 trough and the satellite-implied trough (may or may not coincide; annotate both regardless).

**Caption.** "The monthly satellite composite of Bolivian real activity tracks INE's quarterly GDP in expansions and diverges from it during the 2024–2025 acute crisis window. Composite in z-score on left axis (teal, shaded 68% and 95% bands); INE quarterly GDP growth (y/y) on right axis (rust, stepped). Vertical dashed lines mark the 2020 COVID onset, the 2023 emergence of a parallel foreign-exchange rate, the November 2025 government transition, and the December 2025 fuel-subsidy elimination."

#### Figure 4 — Sectoral decomposition of the contraction

**Purpose.** Show where the contraction is coming from, and where the satellite composite and INE disagree about that. This is where the falsification argument lives sectorally.

**Layout.** Two-panel stacked, 6.5 × 6.0 inches. Top panel: satellite-implied contribution to quarterly GDP growth by channel, stacked bars, 2023 Q1 through 2026 Q1. Bars colored by channel: hydrocarbons (via VNF + YPFB electricity), urban activity (via VIIRS + NO₂ + cement + imports + real IVA), agriculture (via NDVI), residual. Bottom panel: INE's reported sectoral contributions over the same period, same color scheme, stacked bars.

**Annotations.** Horizontal zero line. Where the satellite and INE disagree by more than 1 percentage point for a given quarter-channel, mark with a small arrow on the top panel.

**Caption.** "Sectoral decomposition of quarterly GDP growth. Top: implied from the satellite composite, with channels recovered from factor loadings (Track D). Bottom: INE's reported sectoral contributions. Disagreements between the two decompositions of more than one percentage point are marked."

#### Figure 5 — The manipulation-detection verdict

**Purpose.** Deliver Test 3 (sectoral triangulation) visually. The figure that summarizes the paper's strongest identification claim.

**Layout.** Two-panel side-by-side, 10.0 × 4.0 inches. Left: scatter of log(Σ RH) against log(YPFB gas production), monthly, 2012–2026. Pre-crisis 2012–2022 points hollow slate circles; 2023–2024 points filled ochre; 2025–2026 points filled rust. Fitted line from pre-crisis sample with 95% CI band extrapolated forward. Right: time series of INE hydrocarbon value added (slate), VNF-implied gas production (teal), and YPFB-reported gas production (rust), all indexed to 2019 = 100, 2012–2026.

**Annotations.** Left panel: pre-crisis regression equation and R² in top-left corner. Right panel: callout of the gap between VNF-implied and INE VA in 2024–2026; separate callout of the gap between VNF-implied and YPFB-reported over the same window.

**Caption.** "Test 3 (sectoral triangulation) from §4.3. Left: VNF radiant heat against YPFB gas production, with pre-crisis calibration (2012–2022) extrapolated to the crisis window. Right: INE hydrocarbon value added, YPFB production, and VNF-implied production, indexed to 2019. Divergence between YPFB and VNF implies source-data manipulation; divergence between YPFB and INE value added implies aggregation-level manipulation; divergence of both from VNF implies both."

### Appendix figures

#### Figure A1 — Spatial coverage

**Purpose.** Every spatial choice in the paper is visually defensible. One map, three panels.

**Layout.** Three panels side-by-side, 10.0 × 4.5 inches. Left: Bolivia administrative boundary with 11 urban buffers overlaid on 2019 annual VIIRS SOL (log-scale viridis colormap, the one colormap exception in the paper). Center: Chaco basin boundary with six gas-field markers overlaid on 2019 annual VNF detection-density heatmap. Right: Bolivia with TROPOMI NO₂ rectangles (La Paz–El Alto, Santa Cruz, Cochabamba) and Sentinel-2 cropland zones overlaid on ESA WorldCover 2021.

**Annotations.** Latitude and longitude graticule at 2° intervals. Scale bar on each panel. Small departmental labels in slate, 6 pt.

**Caption.** "Spatial coverage of the four satellite streams. Left: urban nighttime-lights buffers over 2019 VIIRS annual mean. Center: Chaco gas fields over 2019 VNF detection density. Right: TROPOMI NO₂ rectangles and Sentinel-2 cropland zones over ESA WorldCover 2021."

#### Figure A2 — Pre-crisis VNF-to-production calibration, by field

**Purpose.** Full-resolution diagnostic for Track C. Determines whether the paper goes Option 1 or Option 2 in Track E.

**Layout.** Six panels (2×3), 10.0 × 6.5 inches. One panel per field (Margarita, Huacaya, San Alberto, Sábalo, Incahuasi, Aquio; Itaú goes in a seventh panel or in the caption if space is tight). Each panel: scatter of monthly log(Σ RH) against log(YPFB production), 2012–2022 only. OLS fitted line with 95% CI band.

**Annotations.** Each panel reports slope, slope standard error, R², n. Panel title = field name, gas composition (dry or mixed), and vintage.

**Caption.** "Field-level pre-crisis calibration of VIIRS Nightfire radiant heat against YPFB-reported gas production, 2012–2022. Slope, HAC standard error, and R² reported per field."

### Main-text tables

#### Table 1 — Spatial definitions (consolidated)

**Purpose.** Replace current Tables 1, 2, 3 (urban buffers, Chaco fields, TROPOMI ROIs). Add Sentinel-2 zones from Track A. One unified table with four sections.

**Format.** `booktabs` with section headers via `\midrule` and italic section labels in first column. Columns: Name, Latitude, Longitude, Extent (radius km or rectangle size), Notes. Four sections: Urban buffers (11 rows), Gas fields (7 rows), TROPOMI ROIs (3 rows), Cropland zones (5 rows). Total 26 rows including section headers.

#### Table 2 — Single-series elasticities

**Purpose.** Deliver the Figure 2 numbers in tabular form for precise reading and referee reproduction.

**Format.** `booktabs`, columns: Stream, Specification reference, β̂ (HAC SE), n, R², Literature benchmark, Benchmark source. Rows: VIIRS, VNF (pooled and by field), NO₂, NDVI. Stars on β̂ for significance.

#### Table 3 — Satellite composite versus INE and external forecasters

**Purpose.** Communicate the core comparative finding in one table.

**Format.** `booktabs`, columns: Year, INE reported (%), Satellite composite 95% CI (%), IMF forecast (%), World Bank forecast (%), Oxford forecast (%), S&P forecast (%). Rows: 2023, 2024, 2025, 2026. Include a bottom row: cumulative 2023–2026.

#### Table 4 — Manipulation-detection suite

**Purpose.** Deliver the three tests from revised §4.3 (Track B) in one table.

**Format.** `booktabs`, columns: Test, Identifying assumption (one line), Test statistic, p-value or 95% CI, Verdict. Rows: Test 1 (Nov 2025 INE-leadership discontinuity), Test 2 (satellite-vs-external-forecasters residual), Test 3 (VNF–YPFB–INE sectoral triangulation). Verdicts: "Reject null at 5%", "Fail to reject", "Mixed", etc.

### Appendix tables

#### Table A1 — Data vintages and release lags

**Purpose.** Honest accounting of what was known when. Shields the paper from referee concerns about data-mining or look-ahead.

**Format.** `booktabs`, columns: Series, Source, Frequency, Nominal release lag (days), Observed vintage cutoff in this paper, Revision behavior (stable / revised / reclassified). One row per series: INE quarterly GDP, INE IGAE, INE departmental GDP, YPFB field production, IBCH cement, CNDC electricity, SIN tax, Aduana imports, BCB monetary, VIIRS DNB, VIIRS Nightfire, TROPOMI NO₂, Sentinel-2 NDVI.

#### Table A2 — Robustness grid

**Purpose.** One-page summary of the Track G sensitivity exercises. If all columns agree within the CI, the paper is robust and the reader sees it at a glance.

**Format.** `booktabs`, columns: Specification, Trough date, Trough depth (σ below mean), Implied 2025 annual growth (%), Implied 2026 annual growth (%). Rows: Baseline, VIIRS-only, VNF-only, NO₂-only, NDVI-only, VNP46A3 vs. DNB/MONTHLY_V1, VNF radius 1 km / 2 km / 3 km, VNF threshold 1200 K / 1400 K / 1600 K, TROPOMI qa 0.50 / 0.75 / 0.90, one-factor DFM vs. two-factor DFM vs. weighted composite, alternative baseline 2013–2019 vs. 2015–2019 vs. 2017–2019, population weights vs. GDP weights.

### Generation workflow

1. Load the Track F Parquet dataset via the frozen Zenodo snapshot.
2. Build a single Python script `/scripts/figures/make_all_figures.py` that generates every figure from the dataset. Deterministic seeds, no hand-tuned layouts.
3. One utility module `/scripts/figures/lalinterna_style.py` containing the rcParams, palette, and caption templates. Imported by every figure script.
4. Figures output to `/figures/pdf/` and `/figures/png/`. Overwrite-safe with version-stamped filenames.
5. Tables generated via `/scripts/tables/make_all_tables.py` producing `.tex` fragments in `/tables/` that the main paper includes via `\input{tables/table_X.tex}`.
6. A single Makefile target `make figures && make tables && make paper` reproduces the full v2 PDF.

### What this section deliberately excludes

- Interactive or 3D visualizations. The paper is a PDF.
- Nighttime-lights before-and-after RGB overlays. Undercut the econometric register.
- EMBI or parallel-rate charts. Those belong in the Bolivia EWS paper, not here.
- Flare-visibility satellite photographs of the Chaco. Appendix Figure A1 center panel covers the spatial defensibility without the remote-sensing-paper aesthetic.
- Municipality-level heatmaps of growth. That work belongs in the Bolivia Census 2024 mobility book, not here.

### Acceptance criterion

The v2 paper contains exactly five main-text figures, four main-text tables, two appendix figures, two appendix tables. Every figure has a claim-forward caption under 40 words. Every figure regenerates from the frozen dataset via `make figures`. Every table regenerates from the frozen dataset via `make tables`. No stub renderings survive from v1.

---

## Execution order

1. **Track G** (one hour): fix the visible errors. Do first so nobody sees the stub renderings.
2. **Track C** (two to three days): estimate VNF-to-production. This gates Track E framing, §3.1.2 text, and Figure A2.
3. **Track A** (three to five days): add Sentinel-2. This resolves the agricultural blind spot and feeds Track D, Figure 1 panel 4, and Figure 4 agriculture bar.
4. **Track B** (one to two days of writing after Track C results are in): rewrite §4.3 around the three new tests. Feeds Table 4.
5. **Track D** (two to three days): build the two-factor DFM or fall back to the weighted composite. Feeds Figures 3 and 4.
6. **Track E** (one day of writing): commit to falsification or coincident-indicator framing. Revise abstract, §1, §4.3, §7 for consistency.
7. **Track F** (two to three days, can run in parallel with Tracks A–E): freeze the Zenodo dataset and document the La Linterna scraper pipeline. Gates Track H.
8. **Track H** (three to five days, runs after Tracks A–F are complete): generate all figures and tables from the frozen dataset.

Total: roughly three to four weeks of focused work to submission-ready v2.

## Out of scope for this revision

- Real-time operational alerts from the satellite composite (belongs in the dashboard code, not the paper).
- Extensions to Venezuela, Angola, or Afghanistan (belongs in a follow-up paper; mention in conclusion as one sentence).
- Integration with the online-scraped CPI (Cavallo–Rigobon tradition) — cited as future work in §7, not built.
- Sub-municipal disaggregation of the satellite composite.
- Forecasting beyond one-month-ahead nowcasting.

## Standing preferences (non-negotiable)

- LaTeX only, never Word.
- Palatino (mathpazo + TeX Gyre Pagella). Figures in Palatino via matplotlib rcParams.
- No affiliations in author block. Standard disclaimer footnote.
- Acemoglu register: claim-lead paragraphs, causal density 30–40 percent, meta-commentary under 15 percent, zero evaluative adjectives, active voice above 85 percent, specificity with numbers, mean sentence length 18–24 words with high variance.
- Blunt peer feedback on all drafts. No cheerleading. Problems first, praise only if genuinely surprising.
- Treat author as an econometrician who knows the tools; do not explain standard methods.

## Handoff artifacts at v2 submission

- `fires_lights_smog_v2.tex` and `references_v2.bib`, LaTeX source, compiles clean
- `fires_lights_smog_v2.pdf`, full PDF with all figures and tables
- `/figures/pdf/` directory with 5 main + 2 appendix figures, all regenerable
- `/tables/` directory with 4 main + 2 appendix `.tex` table fragments, all regenerable
- `/scripts/figures/` and `/scripts/tables/` with the Python code that produced them, plus `lalinterna_style.py`
- `Makefile` with targets `figures`, `tables`, `paper`, `all`
- Zenodo-archived replication dataset with DOI
- La Linterna dashboard updated to reflect the four-stream composite
- One-page cover letter with the journal pick (Option 1 or Option 2 target from Track E)
- Referee-response-style memo internal to the author listing what changed from v1 to v2 and why
