# Bolivia satellite-proxy pipeline.
# Target: `make all` runs fetch -> process -> anomaly -> index -> publish.
# Same convention as whl.Haiti; credentials in .env (see .env.example).

SHELL := /bin/bash
PY    := python

.PHONY: all fetch process anomaly index publish validate clean help refresh-eog

help:
	@echo "Targets:"
	@echo "  fetch     all three streams: VIIRS (GEE), VNF (EOG), S5P (GEE)"
	@echo "  process   VNF attribution to Chaco flares (local)"
	@echo "  anomaly   deseasonalized anomalies for all three streams"
	@echo "  index     satellite coincident index + DuckDB + benchmark vs INE"
	@echo "  publish   figures + weekly brief + monthly LaTeX report"
	@echo "  validate  quarterly validation suite"
	@echo "  all       fetch -> process -> anomaly -> index -> publish"

all: fetch process anomaly index publish

fetch:
	$(PY) src/00_fetch/fetch_viirs_sol.py
	$(PY) src/00_fetch/fetch_vnf.py
	$(PY) src/00_fetch/fetch_s5p_no2.py
	$(PY) src/00_fetch/fetch_wb_ggfr.py

process:
	$(PY) src/01_process/vnf_attribute.py

anomaly:
	$(PY) src/02_anomaly/viirs_anomaly.py
	$(PY) src/02_anomaly/vnf_calibration.py
	$(PY) src/02_anomaly/vnf_wb_crosscheck.py
	$(PY) src/02_anomaly/s5p_anomaly.py

index:
	$(PY) src/03_index/build_ci.py
	$(PY) src/03_index/benchmark_ine.py
	$(PY) src/03_index/igae_disagreement.py
	$(PY) src/03_index/export_for_dfm.py

publish:
	$(PY) src/04_publish/figures.py
	$(PY) src/04_publish/weekly_brief.py
	$(PY) src/04_publish/monthly_report.py

validate:
	$(PY) src/99_validate/quarterly_validation.py

refresh-eog:
	$(PY) src/00_fetch/refresh_eog_token.py

clean:
	rm -rf data/satellite/*.csv data/satellite/*.json data/satellite/*.duckdb
	rm -rf outputs/*.md outputs/*.tex outputs/*.txt outputs/figures/*.pdf
