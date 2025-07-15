# build_optionable_list.py

import logging
import requests
import json
from dotenv import load_dotenv

# --- Setup ---
load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

# --- Configuration ---
API_KEY = os.getenv("POLYGON_API_KEY")
OUTPUT_FILE = "optionable_tickers.json"
SEMAPHORE = asyncio.Semaphore(100) 
async_client = httpx.AsyncClient(timeout=30)

BASE_URL = "https://api.polygon.io/v3/reference/tickers"

# Parameters to filter optionable stocks
params = {
    "market": "stocks",
    "options": "true",
    "active": "true",
    "limit": 1000,
    "apiKey": API_KEY
}

optionable_stocks = []
next_url = BASE_URL

while next_url:
    response = requests.get(next_url, params=params)
    data = response.json()

    # Append tickers to your list
    optionable_stocks.extend(data.get("results", []))

    # Check for pagination
    next_url = data.get("next_url")
    params = {}  # Clear params for next_url requests (already included in URL)

# Save to JSON file
with open("optionable_stocks.json", "w") as f:
    json.dump(optionable_stocks, f, indent=4)

print(f"Saved {len(optionable_stocks)} optionable stocks to optionable_stocks.json")
