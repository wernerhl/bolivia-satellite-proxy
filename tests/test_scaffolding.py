"""Scaffolding smoke tests — run without credentials.

Confirms config loads, package imports, and that the 11 cities / 7 flares /
3 ROIs are configured with sensible coordinates. `make fetch` requires
credentials and is not covered here.
"""
from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))

from _common import buffers, flares, paths, rois  # noqa: E402


def test_paths_load():
    p = paths()
    assert p["project"]["country_iso3"] == "BOL"
    assert p["infra"]["gee_project"] == "haiti-poverty-monitoring"


def test_buffers_11_cities():
    cities = buffers()
    assert len(cities) == 11
    names = {c["name"] for c in cities}
    assert "la_paz_el_alto" in names
    assert "santa_cruz" in names
    for c in cities:
        assert -23 < c["lat"] < -10
        assert -70 < c["lon"] < -57
        assert c["radius_km"] > 0
        assert c["population"] > 0


def test_flares_7_fields():
    f = flares()
    assert len(f["fields"]) == 7
    for fld in f["fields"]:
        assert -23 < fld["lat"] < -20
        assert -65 < fld["lon"] < -62


def test_rois_3_metros():
    r = rois()
    assert len(r) == 3
    for roi in r:
        assert roi["nw_lat"] > roi["se_lat"]  # NW is north of SE
        assert roi["nw_lon"] < roi["se_lon"]  # NW is west of SE


def test_stream_config_present():
    p = paths()
    s = p["streams"]
    assert s["viirs_sol"]["mask_low"] == 0.5
    assert s["viirs_sol"]["mask_high"] == 200
    assert s["vnf"]["temp_bb_min"] == 1400
    assert s["vnf"]["attribution_radius_km"] == 2.0
    assert s["s5p_no2"]["qa_min"] == 0.75
    assert s["s5p_no2"]["min_valid_days_per_month"] == 15


def test_weights_sum_to_one():
    from importlib import import_module
    sys.path.insert(0, str(REPO / "src" / "03_index"))
    mod = import_module("build_ci")
    assert abs(sum(mod.WEIGHTS.values()) - 1.0) < 1e-9
