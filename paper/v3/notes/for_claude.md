# Notes for Claude — v3 vintage, 2026-04-24

Findings the agent believes are worth flagging before Claude writes
the paper body or fills any `\pend{}` placeholder.

## Empirical findings worth incorporating

### E1-VIIRS — elasticity on the Bolivia departmental panel (2017 base)

Updated 2026-04-25 after fetching the INE 2017-base departmental
chained-volume series from `referencia2017/pib_departamental.html`.

- $\hat\beta = 0.017$, two-way clustered SE $= 0.111$,
  $t = 0.16$, $p = 0.87$, $n = 63$, $R^2 < 0.01$.
- Sample 2018–2024 (post-2017 overlap; 9 departments × 7 years).
- The estimate is essentially zero. The 2018–2024 sub-period is
  dominated by the 2020 COVID shock and the 2024 acute crisis, both
  of which produce large GDP swings without commensurate VIIRS SOL
  movements (urban radiance is much smoother than annual GDP under
  shocks driven by sectoral collapse rather than urban activity).
- Comparison with the previous 1990-base estimate ($\hat\beta = 0.074$
  on 2013–2016, $p = 0.04$): the longer 2017-base sample widens the
  CI and pushes the point estimate toward zero. Both samples are
  short by HSW 2012 cross-country standards.
- Recommend: report both estimates explicitly. Frame the 2018–2024
  result as evidence that VIIRS DNB does not pick up Bolivia's
  recession-magnitude GDP swings 1-for-1, which is consistent with
  the narrative that the contraction is concentrated in extractive
  and construction sectors that are NOT well-proxied by urban
  nighttime lights.

**Mixing 1990-base and 2017-base values produced a spurious
$\hat\beta \approx 1.6$ from the unit-mismatch break across 2016/17.
The estimator now filters to `base_year == 2017` only.**

### E1-NDVI — elasticity on agricultural GVA (2017 base)

Updated with the 2017-base departmental panel:

- $\hat\beta = 0.242$, two-way clustered SE $= 0.161$,
  $t = 1.50$, $p = 0.13$, $n = 28$, $R^2 = 0.11$.
- Sample 2018–2024 (5 zone-mapped departments × 7 years).
- Borderline-significant point estimate consistent with the
  Johnson (2014) US crop-yield benchmark of 0.3–0.5. With seven
  annual observations per zone the inference is fragile; treat as
  preliminary and revisit at R&R as additional years accumulate.

### E2 — two-factor DFM is not runnable this vintage

The two-factor DFM declared in E2 requires nine monthly Parquet
files. Four are available (VIIRS, NO$_2$, NDVI, Aduana) and five are
missing (IBCH cement, CNDC electricity, SIN tax, VNF Chaco, YPFB
gas). Per rule zero, the DFM was not fit on a partial panel. Figures
3 and 4 are placeholders; Table 3 composite column is `---`.

### E3 — manipulation tests all blocked

All three tests require data not yet in the archive. Test 1 cannot
be run with annual WB-GGFR as a substitute for monthly VNF per
brief rule one. Table 4 is entirely `---`.

## Caveats to surface in paper prose

1. The 2017-base INE quarterly series is not published at this
   vintage; everything flowing through `ine_gdp_quarterly.parquet`
   is in 1990 constant prices. Elasticities are invariant under log
   transformation, but absolute-level comparisons with external
   forecasts require rebasing. Cite this in §3.2 or Appendix A.

2. The aduana microdata parsed (26 monthly rows, 2024-01–2026-02)
   covers CIF + FOB + gross weight totals only. The brief's
   import-category breakdown (capital / intermediate / consumption
   / fuel) is not in `aduana_imports_monthly.parquet` yet — the
   parser emits `import_category = "TOTAL"` for every row. Rewire
   once the category crosswalk lands.

3. The pre-revision (v1) fill-paper mechanism silently wrote numbers
   into the abstract via `% BEGIN-AUTO-headline`. Do **not** port
   that pattern into v3. Per the v3 brief, no auto-filled prose.

## What's waiting on external approvals

- **EOG Nightfire license** (D9, F5, FA2, E3-Test-1): submitted
  2026-04-24, typical turnaround days to weeks. Check
  `eogdata.mines.edu/products/vnf/` for token issuance.
- **INE 2017-base departmental series** (D2 rebase): no public
  announcement of timing. The 1990-base series ending in 2016 is
  what runs through the pipeline today.
- **INE monthly IGAE** (D3, E3-Test-2): INE publishes from March
  2026; at submission vintage this is two observations. Test 2 is
  flagged preliminary per brief.

## Rule-zero compliance notes

Placeholder PDFs for figures 3, 4, 5, and A2 contain a single line
reading "Figure pending: requires <input>". They do NOT render fake
axes, fake regression lines, or stream-by-stream "TBD stream not
yet available" text. Reader cannot mistake them for real figures.

All `---` cells in tables 2, 3, 4, A1, A2 are literal three-dash
placeholders, not "inputs_missing" or "0.00" strings.
