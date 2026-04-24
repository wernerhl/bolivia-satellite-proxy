"""Binance P2P USDT/BOB parallel-market rate — dollar premium series.

The Binance P2P public endpoint returns the current order book for a
given pair. We poll BUY-side (USDT→BOB) for retail-size orders and
record the trade-weighted median ask. Intended to run on a daily cron.

The official BCB rate is pegged at 6.96 BOB/USD; the parallel premium
is (P2P / 6.96) − 1. Historical data from the BoliviaEWS archive
should be merged in if available at data/official/dollar_premium_history.csv.

Output: data/official/dollar_premium.csv (date, parallel_rate, premium).
Appends one row per run; deduplicates by date.
"""
from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path

import pandas as pd
import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from _common import abs_path, ensure_dir, load_env  # noqa: E402

load_env()


OFFICIAL_PEG = 6.96
ENDPOINT = "https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search"
HEADERS = {"Content-Type": "application/json", "Accept": "application/json",
           "User-Agent": "Mozilla/5.0 (bolivia-satellite-proxy monitor)"}


def fetch_p2p_median_ask(
    fiat: str = "BOB", asset: str = "USDT", trade_type: str = "BUY",
    rows: int = 20, pay_types: list[str] | None = None,
) -> float | None:
    body = {
        "proMerchantAds": False, "page": 1, "rows": rows,
        "payTypes": pay_types or [], "countries": [],
        "publisherType": None, "asset": asset, "fiat": fiat,
        "tradeType": trade_type,
    }
    r = requests.post(ENDPOINT, headers=HEADERS, data=json.dumps(body), timeout=30)
    r.raise_for_status()
    data = r.json().get("data", [])
    if not data:
        return None
    prices = [float(d["adv"]["price"]) for d in data if d.get("adv", {}).get("price")]
    prices.sort()
    return prices[len(prices) // 2] if prices else None


def main() -> None:
    today = date.today()
    rate = fetch_p2p_median_ask()
    if rate is None:
        print("[warn] no P2P quotes returned; skipping today")
        return
    premium = rate / OFFICIAL_PEG - 1

    out_path = abs_path("data/official/dollar_premium.csv")
    ensure_dir(out_path.parent)

    row = pd.DataFrame([{
        "date": pd.Timestamp(today),
        "parallel_rate_bob_per_usd": rate,
        "official_rate_bob_per_usd": OFFICIAL_PEG,
        "dollar_premium": premium,
    }])
    if out_path.exists():
        old = pd.read_csv(out_path, parse_dates=["date"])
        df = pd.concat([old, row], ignore_index=True)
        df = df.drop_duplicates(subset=["date"], keep="last").sort_values("date")
    else:
        df = row
    df.to_csv(out_path, index=False)
    print(f"[ok] P2P {today.isoformat()}: BOB/USD={rate:.2f} "
          f"premium={premium:+.1%} → {out_path}")


if __name__ == "__main__":
    main()
