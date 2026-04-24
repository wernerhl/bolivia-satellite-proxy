"""Integration — build the satellite-only coincident index.

Aggregation rules (from the agent spec):
  * VIIRS composite = population-weighted mean of per-city anomalies, z-scored
    on each city's baseline
  * VNF composite = z-scored Chaco total log(RH) (single series)
  * S5P composite = simple mean of La Paz + Santa Cruz z-scores
Final CI: 0.40·VIIRS_z + 0.30·VNF_z + 0.30·NO2_z

Also persists a DuckDB database that joins the three streams with any
available official series for downstream benchmark regression.
"""
from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from _common import abs_path, buffers, load_env, paths  # noqa: E402

load_env()


WEIGHTS = {"viirs": 0.40, "vnf": 0.30, "no2": 0.30}


def viirs_composite(anomaly: pd.DataFrame, pop_by_city: dict[str, float]) -> pd.DataFrame:
    df = anomaly.copy()
    df["date"] = pd.to_datetime(df["date"])
    df = df.dropna(subset=["anomaly"])

    # Z-score per city. Center on the pre-2020 baseline (so the baseline
    # period has mean ≈ 0 by construction), but scale by the full-sample
    # sd: baseline residuals from the fitted linear trend have artificially
    # small variance, so using baseline sd produces absurd post-2020
    # z-scores. The spec's manipulation test cares about economic magnitude,
    # which full-sample sd preserves.
    base = df[df["date"].dt.year < 2020]
    mu = base.groupby("city")["anomaly"].mean().rename("mu")
    sd = df.groupby("city")["anomaly"].std(ddof=1).rename("sd")
    df = df.merge(mu, left_on="city", right_index=True, how="left")
    df = df.merge(sd, left_on="city", right_index=True, how="left")
    df["z"] = (df["anomaly"] - df["mu"]) / df["sd"]

    df["pop"] = df["city"].map(pop_by_city).fillna(0.0)
    grp = df.groupby("date")
    composite = grp.apply(
        lambda g: np.average(g["z"], weights=g["pop"]) if g["pop"].sum() > 0 else np.nan,
        include_groups=False,
    ).rename("viirs_z").reset_index()
    return composite


def no2_composite(anomaly: pd.DataFrame) -> pd.DataFrame:
    df = anomaly.copy()
    df["date"] = pd.to_datetime(df["date"])
    metros = df[df["roi"].isin(["la_paz_el_alto", "santa_cruz"])]
    return metros.groupby("date", as_index=False)["z_vs_2019"].mean().rename(
        columns={"z_vs_2019": "no2_z"}
    )


def vnf_composite(anomaly: pd.DataFrame) -> pd.DataFrame:
    df = anomaly.copy()
    df["date"] = pd.to_datetime(df["date"])
    return df[["date", "rh_anomaly_z"]].rename(columns={"rh_anomaly_z": "vnf_z"})


def main() -> None:
    p = paths()
    pop_by_city = {c["name"]: c["population"] for c in buffers()}

    viirs = pd.read_csv(abs_path(p["data"]["viirs_sol_anomaly"]))
    vnf = pd.read_csv(abs_path(p["data"]["vnf_anomaly"]))
    no2 = pd.read_csv(abs_path(p["data"]["s5p_anomaly"]))

    v = viirs_composite(viirs, pop_by_city) if not viirs.empty else pd.DataFrame(columns=["date", "viirs_z"])
    f = vnf_composite(vnf) if not vnf.empty else pd.DataFrame(columns=["date", "vnf_z"])
    n = no2_composite(no2) if not no2.empty else pd.DataFrame(columns=["date", "no2_z"])

    ci = v.merge(f, on="date", how="outer").merge(n, on="date", how="outer").sort_values("date")

    w = WEIGHTS
    ci["ci"] = (
        w["viirs"] * ci["viirs_z"].fillna(0)
        + w["vnf"] * ci["vnf_z"].fillna(0)
        + w["no2"] * ci["no2_z"].fillna(0)
    )
    # If all three missing, CI is NA
    mask_all_na = ci[["viirs_z", "vnf_z", "no2_z"]].isna().all(axis=1)
    ci.loc[mask_all_na, "ci"] = np.nan

    ci = ci[["date", "ci", "viirs_z", "vnf_z", "no2_z"]]
    out_csv = abs_path(p["data"]["ci"])
    ci.to_csv(out_csv, index=False)
    print(f"[ok] wrote {out_csv} ({len(ci)} rows)")

    # DuckDB store — join with any official series that exist
    db_path = abs_path(p["data"]["ci_db"])
    con = duckdb.connect(str(db_path))
    con.execute("CREATE OR REPLACE TABLE ci AS SELECT * FROM ci")
    for key, name in [
        ("official_igae", "igae"),
        ("official_cement", "cement"),
        ("official_cndc", "cndc"),
        ("official_ypfb", "ypfb"),
        ("official_sin", "sin"),
        ("official_aduana", "aduana"),
    ]:
        fp = abs_path(p["data"][key])
        if fp.exists():
            con.execute(f"CREATE OR REPLACE TABLE {name} AS "
                        f"SELECT * FROM read_csv_auto('{fp}')")
    con.close()
    print(f"[ok] wrote DuckDB store {db_path}")


if __name__ == "__main__":
    main()
