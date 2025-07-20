import requests
import datetime
import os

API_KEY = os.getenv("POLYGON_API_KEY")
BASE_URL = "https://api.polygon.io"
HEADERS = {"Authorization": f"Bearer {API_KEY}"}

# STEP 1: Get Highly Liquid Stocks
def get_liquid_stocks_from_snapshot(min_volume=1000000):
    url = f"{BASE_URL}/v2/snapshot/locale/us/markets/stocks/tickers"
    params = {"limit": 250}  # Adjust or paginate for more
    response = requests.get(url, headers=HEADERS, params=params).json()
    
    tickers = [
        t["ticker"] for t in response.get("tickers", [])
        if t.get("day", {}).get("volume", 0) >= min_volume
    ]
    return tickers

# STEP 2: Check Options Data (IV, Open Interest)
def analyze_options(ticker):
    url = f"{BASE_URL}/v3/snapshot/options/{ticker}"
    response = requests.get(url, headers=HEADERS).json()
    options = response.get("results", [])
    filtered = []
    for opt in options:
        if opt.get("greeks", {}).get("iv") and opt.get("open_interest", 0) > 100:
            filtered.append({
                "symbol": opt.get("details", {}).get("ticker"),
                "iv": opt["greeks"]["iv"],
                "open_interest": opt["open_interest"],
                "strike_price": opt.get("details", {}).get("strike_price"),
                "expiration": opt.get("details", {}).get("expiration_date")
            })
    return filtered

# STEP 3: Check Earnings Calendar
def has_upcoming_earnings(ticker):
    today = datetime.date.today()
    future_date = today + datetime.timedelta(days=14)
    url = f"{BASE_URL}/v1/meta/symbols/{ticker}/news"
    response = requests.get(url, headers=HEADERS).json()
    for item in response.get("results", []):
        if "earnings" in item.get("title", "").lower():
            published = datetime.datetime.strptime(item["published_utc"], "%Y-%m-%dT%H:%M:%SZ").date()
            if today <= published <= future_date:
                return True
    return False

# COMBINED FUNCTION
def find_candidates():
    candidates = []
    tickers = get_liquid_stocks_from_snapshot()
    print(f"Checking {len(tickers)} stocks...")

    for ticker in tickers:
        if has_upcoming_earnings(ticker):
            continue
        options = analyze_options(ticker)
        if options:
            candidates.append({
                "ticker": ticker,
                "options": options[:3]  # Take top 3 contracts
            })

    return candidates

# RUN
if __name__ == "__main__":
    results = find_candidates()
    for stock in results:
        print(f"\n{stock['ticker']}")
        for opt in stock["options"]:
            print(f"  → {opt['symbol']} | IV: {opt['iv']:.2f} | OI: {opt['open_interest']}")
