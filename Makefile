# Bolivia satellite-proxy pipeline.
# `make all` runs the full build: fetch → process → anomaly → index →
# econometrics → paper assets → paper PDF. Credentials in .env.
# Reuses whl.Haiti GCP project, bucket, and service-account key.

SHELL := /bin/bash
PY    := .venv/bin/python

.PHONY: all fetch process anomaly index econometrics paper-assets paper paper-v2 figures tables \
        publish validate clean help refresh-eog test-scaffold

help:
	@echo "Targets:"
	@echo "  fetch          VIIRS + VNF + S5P (GEE/EOG) + WB GGFR + Binance P2P"
	@echo "  process        VNF flare attribution"
	@echo "  anomaly        per-stream anomalies + VNF calibration + WB cross-check"
	@echo "  index          CI + DuckDB + INE benchmark + IGAE disagreement + DFM export"
	@echo "  econometrics   single-series elasticities + DFM + BBQ + Markov-switching + manipulation tests"
	@echo "  paper-assets   paper tables (tex) + paper figures (pdf) + inline patch"
	@echo "  paper          alias for paper-v2 (compile paper/v2/fires_lights_smog.pdf)"
	@echo "  paper-v1       compile paper/v1/fires_lights_smog.pdf (pristine pre-revision)"
	@echo "  paper-v2       regen figures + tables, then compile paper/v2/fires_lights_smog.pdf"
	@echo "  figures        regenerate paper/v2/figures/pdf/*.pdf"
	@echo "  tables         regenerate paper/v2/tables/*.tex"
	@echo "  publish        brief + monthly LaTeX report + social post"
	@echo "  validate       quarterly validation suite"
	@echo "  refresh-eog    refresh EOG_TOKEN from EOG_USER/EOG_PASS"
	@echo "  test-scaffold  pytest config + imports"
	@echo "  all            fetch -> process -> anomaly -> index -> econometrics -> paper-assets -> paper"

all: fetch process anomaly index econometrics paper-assets paper

fetch:
	$(PY) src/00_fetch/fetch_viirs_sol.py
	$(PY) src/00_fetch/fetch_vnf.py
	$(PY) src/00_fetch/fetch_s5p_no2.py
	$(PY) src/00_fetch/fetch_s2_ndvi.py
	$(PY) src/00_fetch/fetch_wb_ggfr.py
	$(PY) src/00_fetch/fetch_binance_p2p.py
	$(PY) src/00_fetch/fetch_official_bolivia.py

process:
	$(PY) src/01_process/vnf_attribute.py

anomaly:
	$(PY) src/02_anomaly/viirs_anomaly.py
	$(PY) src/02_anomaly/vnf_calibration.py
	$(PY) src/02_anomaly/vnf_wb_crosscheck.py
	$(PY) src/02_anomaly/s5p_anomaly.py
	$(PY) src/02_anomaly/s2_ndvi_anomaly.py

index:
	$(PY) src/03_index/build_ci.py
	$(PY) src/03_index/benchmark_ine.py
	$(PY) src/03_index/igae_disagreement.py
	$(PY) src/03_index/pipeline_alerts.py
	$(PY) src/03_index/export_for_dfm.py

econometrics:
	$(PY) src/05_econometrics/elasticities.py
	$(PY) src/05_econometrics/vnf_calibration_field.py
	$(PY) src/05_econometrics/dfm.py
	$(PY) src/05_econometrics/dfm_twofactor.py
	$(PY) src/05_econometrics/recession_dating.py
	$(PY) src/05_econometrics/manipulation_tests.py

paper-assets:
	$(PY) src/06_paper/tables.py
	$(PY) src/06_paper/paper_figures.py
	$(PY) src/06_paper/fill_paper.py

paper: paper-v2

paper-v1:
	cd paper/v1 && pdflatex -interaction=nonstopmode fires_lights_smog.tex && \
	               bibtex fires_lights_smog && \
	               pdflatex -interaction=nonstopmode fires_lights_smog.tex && \
	               pdflatex -interaction=nonstopmode fires_lights_smog.tex

figures:
	$(PY) scripts/figures/make_all_figures.py

tables:
	$(PY) scripts/tables/make_all_tables.py

paper-v2: figures tables
	cd paper/v2 && pdflatex -interaction=nonstopmode fires_lights_smog.tex && \
	               bibtex fires_lights_smog && \
	               pdflatex -interaction=nonstopmode fires_lights_smog.tex && \
	               pdflatex -interaction=nonstopmode fires_lights_smog.tex

publish:
	$(PY) src/04_publish/figures.py
	$(PY) src/04_publish/weekly_brief.py
	$(PY) src/04_publish/monthly_report.py

validate:
	$(PY) src/99_validate/quarterly_validation.py

refresh-eog:
	$(PY) src/00_fetch/refresh_eog_token.py

freeze-zenodo:
	$(PY) src/00_fetch/freeze_zenodo_dataset.py

dashboard-vintage:
	$(PY) src/04_publish/dashboard_vintage.py

test-scaffold:
	$(PY) -m pytest tests/ -v

clean:
	rm -rf data/satellite/*.csv data/satellite/*.json data/satellite/*.duckdb
	rm -rf outputs/*.md outputs/*.tex outputs/*.txt outputs/figures/*.pdf
	rm -rf paper/v2/tables/*.tex paper/v2/figures/pdf/*.pdf paper/v2/figures/png/*.png
	cd paper/v1 && latexmk -C 2>/dev/null || true
	cd paper/v2 && latexmk -C 2>/dev/null || true
