# Notes for Claude — v3 vintage, 2026-04-24

Findings the agent believes are worth flagging before Claude writes
the paper body or fills any `\pend{}` placeholder.

## Empirical findings worth incorporating

### E1-VIIRS — elasticity on the Bolivia departmental panel

Estimated equation (1) on 9 departments × 4 usable years of overlap
(VIIRS starts 2012-04, INE departmental panel ends 2016 in the 1990
base; the 2017-base departmental series has not been released).

- $\hat\beta = 0.074$, two-way clustered SE $= 0.036$,
  $t = 2.05$, $p = 0.040$, $n = 36$, $R^2 = 0.13$.
- Sample 2013–2016 (overlap of VIIRS start and 1990-base end).
- HSW 2012 cross-country benchmark is $\approx 0.30$. Bolivia comes in
  lower, consistent with single-country attenuation documented
  in Gibson et al. (2021) and with the short four-year panel.

### E1-NDVI — elasticity on agricultural GVA

Estimated equation (4) on 5 zone-derived departments × 3 usable
years. Two-way clustering collapses with that few clusters; only the
point estimate is informative.

- $\hat\beta = 0.169$, $n = 12$, $R^2 = 0.20$. SE and $p$ not
  reportable under the declared specification.
- Claude: this is a point estimate without credible inference.
  Consider narrowing the paper's §4.1 Eq.~(4) prose to "the point
  estimate is consistent with the Johnson (2014) crop-yield
  benchmark of 0.3–0.5" rather than claiming statistical
  significance. Or wait on the 2017-base departmental series.

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
