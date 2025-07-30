#  python3 -m venv venv
#  source venv/bin/activate
#  pip3 install requests
#  pip3 install tqdm
#  pip3 install tabulate
#  pip3 install pyyaml
#  python3 markets_instruments_V2.py

#!/usr/bin/env python3
import requests
import time
import json
import os
import yaml
from datetime import datetime, timedelta, timezone
from math import ceil

from tqdm import tqdm
from tabulate import tabulate

# ─── LOAD CONFIG ───────────────────────────────────────────────────────────
with open("config.yaml") as f:
    cfg = yaml.safe_load(f)

API_KEY = cfg.get("coindesk", {}).get("api_key")
if not API_KEY:
    raise RuntimeError("❌ Missing `coindesk.api_key` in config.yaml")

# ─── CONFIG ────────────────────────────────────────────────────────────────
BASE_URL   = "https://data-api.coindesk.com/futures/v1/historical/open-interest/minutes"
HEADERS    = {"Accept": "application/json", "X-API-Key": API_KEY}
OUTPUT_DIR = "open_interest_6mo"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ─── PARAMETERS ────────────────────────────────────────────────────────────
DAYS        = 180   # 6 months ≈ 180 days
CHUNK_SIZE  = 2000  # max points per minute-endpoint call
exchange    = "okex" 
instrument  = "BTC-USDT-VANILLA-PERPETUAL"

# ─── DERIVED VALUES ────────────────────────────────────────────────────────
total_minutes  = DAYS * 1440                                    # ≈259200
expected_calls = ceil(total_minutes / CHUNK_SIZE)               # ≈130
start_ts       = int((datetime.now(timezone.utc) - timedelta(days=DAYS)).timestamp())
to_ts          = int(datetime.now(timezone.utc).timestamp())

print(f"→ Backfilling {DAYS} days ({total_minutes:,} minutes) in {expected_calls} calls...")

# ─── FETCH & PAGINATE WITH PROGRESS BAR ────────────────────────────────────
all_data   = []
call_count = 0
start_time = time.time()

# initialize progress bar
pbar = tqdm(total=expected_calls, desc="Backfill Progress", unit="call")

while True:
    remaining_minutes = (to_ts - start_ts) // 60
    if remaining_minutes <= 0:
        break

    this_limit = min(CHUNK_SIZE, remaining_minutes)
    params = {
        "market":        exchange,
        "instrument":    instrument,
        "groups":        "ID,MAPPING,OHLC",   # 1-min OHLC only
        "limit":         this_limit,
        "aggregate":     1,
        "fill":          "true",
        "apply_mapping": "true",
        "to_ts":         to_ts,
    }

    resp = requests.get(BASE_URL, headers=HEADERS, params=params, timeout=30)
    try:
        resp.raise_for_status()
    except requests.HTTPError:
        print(f"\n❌ HTTP {resp.status_code} error:\n{resp.text}")
        break

    batch = resp.json().get("Data", [])
    if not batch:
        print("\n⚠ No more data returned; stopping early.")
        break

    all_data.extend(batch)
    call_count += 1

    # step back to earliest timestamp minus 60s
    earliest_ts = min(item["TIMESTAMP"] for item in batch)
    to_ts = earliest_ts - 60

    pbar.update(1)
    time.sleep(1)  # throttle to respect rate limits

pbar.close()
elapsed_total = time.time() - start_time

print(f"\n→ Completed: {len(all_data):,} total bars in {call_count} calls ({elapsed_total:.1f}s)")

# ─── SAVE TO FILE ────────────────────────────────────────────────────────
date_tag = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
fname = f"{OUTPUT_DIR}/{exchange}_{instrument.replace('/', '-')}_1min_{DAYS}d_{date_tag}.json"

with open(fname, "w") as f:
    json.dump(all_data, f, indent=2)

print(f"💾 Saved {len(all_data):,} bars to {fname}")

# ─── TABULAR SUMMARY ──────────────────────────────────────────────────────
file_size_mb = os.path.getsize(fname) / (1024 * 1024)
if all_data:
    first_ts = min(item["TIMESTAMP"] for item in all_data)
    last_ts  = max(item["TIMESTAMP"] for item in all_data)
    first_dt = datetime.fromtimestamp(first_ts, timezone.utc).strftime("%Y-%m-%d %H:%M")
    last_dt  = datetime.fromtimestamp(last_ts,  timezone.utc).strftime("%Y-%m-%d %H:%M")

    summary = [
        ["Days backfilled", DAYS],
        ["Total minutes",   f"{total_minutes:,}"],
        ["API calls",       call_count],
        ["Run time (s)",    f"{elapsed_total:.1f}"],
        ["File size (MB)",  f"{file_size_mb:.1f}"],
        ["Date range (UTC)", f"{first_dt} → {last_dt}"]
    ]
    print("\n" + tabulate(summary, headers=["Metric", "Value"], tablefmt="github"))


# # Hardcoded exchange:instrument mappings based on test.py
# btc_perp_mapping = {
#     "binance": ["BTC-USDT-VANILLA-PERPETUAL", "BTC-USD-INVERSE-PERPETUAL"], DONE
#     "hyperliquid": ["BTC-USDT-QUANTO-PERPETUAL"], DONE
#     "bybit": ["BTC-USD-INVERSE-PERPETUAL", "BTC-USDC-VANILLA-PERPETUAL", "BTC-USDT-VANILLA-PERPETUAL"],
#     "bitget": ["BTC-USDT-VANILLA-PERPETUAL", "BTC-USDC-VANILLA-PERPETUAL"],
#     "okex": ["BTC-USDC-VANILLA-PERPETUAL", "BTC-USD-INVERSE-PERPETUAL", "BTC-USDT-VANILLA-PERPETUAL"]
#     # CME not included due to earlier 400 errors, probably due to the name being futures, not perpetuals.
# }