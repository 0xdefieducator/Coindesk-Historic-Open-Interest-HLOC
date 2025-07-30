#  python3 -m venv venv
#  source venv/bin/activate
#  pip3 install requests
#  pip3 install pandas
#  python3 markets_instruments.py

import requests
import pandas as pd
import os
import json
import time
from datetime import datetime, timezone

os.makedirs("open_interest_data", exist_ok=True)

API_KEY = "xxx"  # ← Replace with your actual CoinDesk API key
BASE_URL = "https://data-api.coindesk.com/futures/v1/historical/open-interest/days"

HEADERS = {
    "Accept": "application/json",
    "X-API-Key": API_KEY
}

# Hardcoded exchange:instrument mappings based on test.py
btc_perp_mapping = {
    "binance": ["BTC-USDT-VANILLA-PERPETUAL"],
    "hyperliquid": ["BTC-USDT-QUANTO-PERPETUAL"],
    "bybit": ["BTC-USD-INVERSE-PERPETUAL", "BTC-USDC-VANILLA-PERPETUAL", "BTC-USDT-VANILLA-PERPETUAL"],
    "bitget": ["BTC-USDT-VANILLA-PERPETUAL", "BTC-USDC-VANILLA-PERPETUAL"],
    "okex": ["BTC-USDC-VANILLA-PERPETUAL", "BTC-USD-INVERSE-PERPETUAL", "BTC-USDT-VANILLA-PERPETUAL"]
    # CME not included due to earlier 400 errors, probably due to the name being futures, not perpetuals.
}

OUTPUT_DIR = "open_interest_data"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ─── FETCH ─────────────────────────────────────────────────────────────────
def fetch_open_interest(exchange: str, instrument: str, days: int = 7) -> list:
    """
    Fetch the last `days` of daily open-interest OHLC data for the given market/instrument.
    Returns the list in json['Data'].
    """
    now_ts = int(datetime.now(timezone.utc).timestamp())
    params = {
        "market": exchange,
        "instrument": instrument,
        "groups":   "ID,MAPPING,OHLC",  # adjust groups as needed (left out: OHLC_MESSAGE,MESSAGE)
        "limit":    days,
        "aggregate":1,
        "fill":     "true",
        "apply_mapping": "true",
        "to_ts":    now_ts,
    }

    resp = requests.get(BASE_URL, headers=HEADERS, params=params)
    resp.raise_for_status()
    data = resp.json()
    records = data.get("Data", [])
    if not records:
        print(f"⚠ No data returned for {exchange} – {instrument}")
    return records

# ─── SAVE ──────────────────────────────────────────────────────────────────
def save_json(data: list, exchange: str, instrument: str):
    """
    Writes only the list of daily OI records to a JSON file named with YYYYMMDD.
    """
    today = datetime.now(timezone.utc).strftime("%Y%m%d")
    safe_instr = instrument.replace("/", "-")
    filename = os.path.join(OUTPUT_DIR, f"{exchange}_{safe_instr}_{today}.json")

    with open(filename, "w") as f:
        json.dump(data, f, indent=2)
    print(f"✅ Saved {len(data)} records to {filename}")

# ─── MAIN ──────────────────────────────────────────────────────────────────
def main():
    for exchange, instruments in btc_perp_mapping.items():
        for instr in instruments:
            try:
                print(f"→ Fetching daily OI for {exchange} – {instr}")
                records = fetch_open_interest(exchange, instr, days=7)
                if records:
                    save_json(records, exchange, instr)
            except Exception as e:
                print(f"❌ Failed {exchange} – {instr}: {e}")

if __name__ == "__main__":
    main()