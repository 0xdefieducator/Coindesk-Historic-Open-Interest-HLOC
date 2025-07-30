import requests

API_KEY = "xxx"  # Replace with actual key
BASE_URL = "https://data-api.coindesk.com/futures/v1/markets/instruments"

TARGET_EXCHANGES = {
    "cme": "CME",
    "binance": "BINANCE",
    "hyperliquid": "HYPERLIQUID",
    "bybit": "BYBIT",
    "bitget": "BITGET",
    "okex": "OKEX"  # Note: it's 'okex', not 'okx'
}

HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json; charset=UTF-8",
    "X-API-Key": API_KEY
}

btc_perpetuals = {}

for market_slug, display_name in TARGET_EXCHANGES.items():
    print(f"\n→ Fetching instruments for: {display_name}")
    try:
        response = requests.get(
            BASE_URL,
            headers=HEADERS,
            params={
                "market": market_slug,
                "instrument_status": "ACTIVE"
            }
        )
        response.raise_for_status()

        instruments = response.json().get("Data", {}).get(market_slug, {}).get("instruments", {})

        btc_perps = [
            symbol for symbol, meta in instruments.items()
            if "BTC" in symbol.upper()
            and "PERPETUAL" in symbol.upper()
            and meta.get("INSTRUMENT_STATUS") == "ACTIVE"
        ]

        if btc_perps:
            btc_perpetuals[display_name] = btc_perps
            print(f"✅ Found {len(btc_perps)} BTC perpetuals: {btc_perps}")
        else:
            print(f"⚠ No BTC perpetuals found for {display_name}")

    except requests.exceptions.HTTPError as e:
        print(f"❌ HTTP error for {display_name}: {e}")
    except Exception as e:
        print(f"❌ General error for {display_name}: {e}")

# Final Output
print("\n📦 Final BTC Perpetual Mapping:")
for exchange, pairs in btc_perpetuals.items():
    print(f"{exchange}: {pairs}")


# # Hardcoded exchange:instrument mappings based on test.py
# btc_perp_mapping = {
#     "binance": ["BTC-USDT-VANILLA-PERPETUAL"],
#     "hyperliquid": ["BTC-USDT-QUANTO-PERPETUAL"],
#     "bybit": ["BTC-USD-INVERSE-PERPETUAL", "BTC-USDC-VANILLA-PERPETUAL", "BTC-USDT-VANILLA-PERPETUAL"],
#     "bitget": ["BTC-USDT-VANILLA-PERPETUAL", "BTC-USDC-VANILLA-PERPETUAL"],
#     "okex": ["BTC-USDC-VANILLA-PERPETUAL", "BTC-USD-INVERSE-PERPETUAL", "BTC-USDT-VANILLA-PERPETUAL"]
#     # CME not included due to earlier 400 errors, probably due to the name being futures, not perpetuals.
# }