# Bolivia Satellite-Proxy Activity Pipeline

Three satellite-derived monthly activity indicators for Bolivia, deseasonalized
and aggregated into a single coincident index that cross-checks INE official
statistics and feeds the La Linterna EWS dashboard:

1. **VIIRS DNB monthly sum-of-lights** over 11 city buffers (urban activity)
2. **VIIRS Nightfire radiant heat** over the Chaco gas fields (upstream hydrocarbons)
3. **Sentinel-5P TROPOMI NO₂** over La Paz–El Alto, Santa Cruz, Cochabamba (traffic + combustion)

Full agent spec in `bolivia_satellite_proxy_agent_prompt.md`.

## Infrastructure

This project **reuses the Haiti/Gaza GCP project, service account, and GCS bucket**
to avoid a second billing setup. Bolivia outputs live under `gs://$GCS_BUCKET/bolivia/`.

```
GCP_PROJECT_ID=haiti-poverty-monitoring
GCS_BUCKET=haiti-sae-2026
GOOGLE_APPLICATION_CREDENTIALS=/Users/whl/secrets/haiti-sae-runner.json
```

The only net-new credential is `EOG_TOKEN` for the Nightfire product
(not on GEE; request at https://eogdata.mines.edu/).

## Quick start

```bash
cp .env.example .env              # EOG_TOKEN is the only new value to fill
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

make fetch       # VIIRS (GEE) + VNF (EOG) + S5P (GEE)
make process     # VNF flare attribution
make anomaly     # deseasonalized anomalies per stream
make index       # satellite CI + DuckDB + INE benchmark
make publish     # figures + weekly brief + monthly LaTeX report
```

Or `make all`.

## Outputs

```
data/satellite/viirs_sol_monthly.csv     Stream 1 raw monthly
data/satellite/viirs_sol_anomaly.csv     Stream 1 anomaly
data/satellite/vnf_chaco_monthly.csv     Stream 2 monthly per field
data/satellite/vnf_chaco_anomaly.csv     Stream 2 calibration + manipulation flag
data/satellite/s5p_no2_monthly.csv       Stream 3 monthly
data/satellite/s5p_no2_anomaly.csv       Stream 3 multiplicative anomaly
data/satellite/bolivia_satellite_ci.csv  Coincident index
data/satellite/bolivia_satellite_ci.duckdb   Store joining satellite + official
outputs/satellite_bolivia_brief.md       Monday 06:00 La Paz weekly brief
outputs/satellite_bolivia_monthly_report.tex   First Monday of month
outputs/figures/*.pdf                    TeX Gyre Pagella figures
```

## Cadence

- **Weekly** (Monday 06:00 America/La_Paz): brief + figures + social draft
- **Monthly** (first Monday after all three streams update): LaTeX report
- **Quarterly**: run `make validate` to confirm HSW elasticity, VNF–YPFB corr,
  and S5P–fuel-sales corr thresholds. Any failure halts publication.

## Disclaimer

Opinions do not reflect affiliated institutions; all errors are the author's.
Contact: wernerhl@gmail.com.
