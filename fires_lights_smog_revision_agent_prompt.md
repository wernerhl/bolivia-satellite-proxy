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

## Execution order

1. **Track G** (one hour): fix the visible errors. Do first so nobody sees the stub renderings.
2. **Track C** (two to three days): estimate VNF-to-production. This gates Track E framing and §3.1.2 text.
3. **Track A** (three to five days): add Sentinel-2. This resolves the agricultural blind spot and must feed Track D.
4. **Track B** (one to two days of writing after Track C results are in): rewrite §4.3 around the three new tests.
5. **Track D** (two to three days): build the two-factor DFM or fall back to the weighted composite.
6. **Track E** (one day of writing): commit to falsification or coincident-indicator framing. Revise abstract, §1, §4.3, §7 for consistency.
7. **Track F** (two to three days, can run in parallel with Tracks A–E): freeze the Zenodo dataset and document the La Linterna scraper pipeline.

Total: roughly two to three weeks of focused work to submission-ready v2.

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
- Zenodo-archived replication dataset with DOI
- La Linterna dashboard updated to reflect the four-stream composite
- One-page cover letter with the journal pick (Option 1 or Option 2 target from Track E)
- Referee-response-style memo internal to the author listing what changed from v1 to v2 and why
