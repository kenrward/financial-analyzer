import os
import logging
import requests
import json
from dotenv import load_dotenv

# --- Setup ---
load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

API_KEY = os.getenv("POLYGON_API_KEY")

BASE_URL = "https://api.polygon.io/v3/reference/tickers"
PRICE_URL_TEMPLATE = "https://api.polygon.io/v2/aggs/ticker/{ticker}/prev"

params = {
    "market": "stocks",
    "options": "true",
    "active": "true",
    "limit": 1000,
    "apiKey": API_KEY
}

filtered_tickers = []
next_url = BASE_URL

while next_url:
    response = requests.get(next_url, params=params)
    data = response.json()

    tickers = data.get("results", [])

    for ticker in tickers:
        symbol = ticker.get("ticker")
        if not symbol:
            continue

        # Get previous close price
        price_url = PRICE_URL_TEMPLATE.format(ticker=symbol)
        price_resp = requests.get(price_url, params={"apiKey": API_KEY})
        price_data = price_resp.json()

        try:
            close_price = price_data["results"][0]["c"]
            if close_price > 50:
                filtered_tickers.append(symbol)
        except (KeyError, IndexError):
            logging.warning(f"No price data for {symbol}")

    next_url = data.get("next_url")
    params = {}  # Clear pagination params

# --- Save as JSON array ---
with open("filtered_optionable_tickers.json", "w") as f:
    json.dump(filtered_tickers, f)

print(f"Saved {len(filtered_tickers)} tickers to filtered_optionable_tickers.json")
