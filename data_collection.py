# data_collection.py

import os
import time
import requests
import pandas as pd
from polygon import RESTClient
from datetime import date, timedelta
import json

# --- Import configuration from config.py ---
try:
    from config import (
        POLYGON_API_KEY, DATA_DIRECTORY, STOCK_UNIVERSE,
        ENABLE_SCREENER, MIN_OPTIONS_VOLUME, MIN_IMPLIED_VOLATILITY,
        REQUIRE_RECENT_NEWS
    )
except ImportError:
    print("Error: config.py not found or is missing variables.")
    exit()

def get_sp500_tickers():
    """Scrapes the S&P 500 tickers from the Wikipedia page."""
    print("Fetching S&P 500 tickers from Wikipedia...")
    try:
        url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
        tables = pd.read_html(url)
        sp500_df = tables[0]
        tickers = sp500_df['Symbol'].tolist()
        print(f"Successfully fetched {len(tickers)} S&P 500 tickers.")
        return [ticker.replace('.', '-') for ticker in tickers]
    except Exception as e:
        print(f"Error fetching S&P 500 tickers: {e}")
        return []

def get_qqq_tickers():
    """Fetches the Nasdaq-100 (QQQ) constituent tickers."""
    print("Fetching Nasdaq-100 (QQQ) tickers...")
    try:
        url = 'https://www.nasdaq.com/files/Nasdaq-100_component_stock_list.csv'
        df = pd.read_csv(url)
        tickers = df['Symbol'].tolist()
        print(f"Successfully fetched {len(tickers)} Nasdaq-100 tickers.")
        return tickers
    except Exception as e:
        print(f"Error fetching Nasdaq-100 tickers: {e}")
        return []
# --- NEW: Screener Function ---



# In data_collection.py, replace the screen_tickers function

def screen_tickers(client, initial_tickers):
    """
    Scans an initial list of tickers and filters them based on options activity and news.
    This version uses the previous day's close for a more reliable price.
    """
    print(f"\n--- Running Screener on {len(initial_tickers)} Tickers ---")
    hot_list = []
    base_url = "https://api.polygon.io"

    for i, ticker in enumerate(initial_tickers):
        print(f"  ({i+1}/{len(initial_tickers)}) Analyzing {ticker}...")
        try:
            # Step 1: Get a reliable underlying price
            underlying_price = None
            stock_snapshot_url = f"{base_url}/v2/snapshot/locale/us/markets/stocks/tickers/{ticker}?apiKey={POLYGON_API_KEY}"
            stock_response = requests.get(stock_snapshot_url)
            if stock_response.status_code == 200:
                stock_data = stock_response.json()
                if stock_data.get('ticker') and stock_data['ticker'].get('prevDay'):
                    underlying_price = stock_data['ticker']['prevDay'].get('c')

            if not underlying_price:
                print(f"    > Could not determine underlying price for {ticker}. Skipping.")
                continue
            
            print(f"    > Underlying price: {underlying_price}")

            # Step 2: Get Options data
            options_url = f"{base_url}/v3/snapshot/options/{ticker}?apiKey={POLYGON_API_KEY}"
            options_response = requests.get(options_url)
            
            if options_response.status_code != 200:
                print(f"    > No options data found for {ticker}.")
                continue

            options_data = options_response.json()
            total_volume, high_iv_found = 0, False

            if "results" not in options_data or not options_data["results"]:
                print(f"    > No options contracts in snapshot for {ticker}.")
                continue

            for contract in options_data["results"]:
                total_volume += contract.get("day", {}).get("volume", 0)
                strike_price = contract.get("details", {}).get("strike_price")
                if abs(strike_price - underlying_price) / underlying_price < 0.10:
                    iv = contract.get("implied_volatility", 0)
                    if iv > MIN_IMPLIED_VOLATILITY:
                        high_iv_found = True
            
            print(f"    > Options Volume: {total_volume}, High IV Found: {high_iv_found}")

            # Step 3: Check criteria with corrected logic
            if total_volume > MIN_OPTIONS_VOLUME and high_iv_found:
                print(f"    > {ticker} meets Volume/IV criteria.")
                
                # --- CORRECTED LOGIC BLOCK ---
                if not REQUIRE_RECENT_NEWS:
                    print(f"    *** {ticker} is a HOT TICKER! (News not required) ***")
                    hot_list.append(ticker)
                else:
                    # This block now only runs if news is required
                    print("    > Checking for recent news...")
                    one_week_ago = (date.today() - timedelta(days=7)).strftime('%Y-%m-%d')
                    news = client.list_ticker_news(ticker, published_utc_gte=one_week_ago, limit=1)
                    has_catalyst = any(news)

                    if has_catalyst:
                        print(f"    *** {ticker} is a HOT TICKER! Adding to list. ***")
                        hot_list.append(ticker)
                    else:
                        print(f"    > No recent news found for {ticker}.")
            else:
                print(f"    > {ticker} does not meet screening criteria.")

        except Exception as e:
            print(f"    > An unexpected error occurred while screening {ticker}: {e}")
            
    print(f"\nScreening complete. Found {len(hot_list)} hot tickers.")
    return hot_list

def fetch_price_data(client, tickers, start_date, end_date):
    """Fetches daily open/close prices for a list of tickers from Polygon.io."""
    all_price_data = {}
    print(f"\nFetching price data for {len(tickers)} tickers from {start_date} to {end_date}...")
    for i, ticker in enumerate(tickers):
        try:
            aggs = client.get_aggs(ticker, 1, "day", start_date, end_date)
            all_price_data[ticker] = [
                {"date": date.fromtimestamp(agg.timestamp / 1000).strftime('%Y-%m-%d'), "open": agg.open, "close": agg.close}
                for agg in aggs
            ]
            print(f"  ({i+1}/{len(tickers)}) Fetched prices for {ticker}")
        except Exception as e:
            print(f"  ({i+1}/{len(tickers)}) Could not fetch price data for {ticker}: {e}")
        time.sleep(0.5)
    return all_price_data

def fetch_news_data(client, tickers):
    """
    Fetches news articles for a list of tickers from Polygon.io,
    now including the sentiment data from the 'insights' field.
    """
    all_news_data = []
    print(f"\nFetching news data (with sentiment) for {len(tickers)} tickers...")

    for i, ticker in enumerate(tickers):
        try:
            # list_ticker_news will return the full news object, including insights
            news_items = client.list_ticker_news(ticker, limit=25) 
            
            for news in news_items:
                # Extract sentiment from the insights array, if it exists
                sentiment_score = 0 # Default to neutral
                sentiment_reasoning = ""
                if news.insights:
                    # An article can have insights on multiple tickers, we'll take the first one.
                    insight = news.insights[0]
                    if insight.sentiment == "positive":
                        sentiment_score = 1
                    elif insight.sentiment == "negative":
                        sentiment_score = -1
                    
                    sentiment_reasoning = insight.sentiment_reasoning

                all_news_data.append({
                    "ticker": ticker,
                    "published_utc": news.published_utc,
                    "title": news.title,
                    "summary": getattr(news, 'description', 'No summary available.'),
                    "polygon_sentiment_score": sentiment_score,
                    "polygon_sentiment_reasoning": sentiment_reasoning
                })
            
            print(f"  ({i+1}/{len(tickers)}) Fetched news for {ticker}")

        except Exception as e:
            print(f"  ({i+1}/{len(tickers)}) Could not fetch news data for {ticker}: {e}")
        
        time.sleep(0.5)
        
    return all_news_data

if __name__ == "__main__":
    if not os.path.exists(DATA_DIRECTORY):
        os.makedirs(DATA_DIRECTORY)

    polygon_client = RESTClient(POLYGON_API_KEY)

    # Step 1: Get the initial list of tickers
    if STOCK_UNIVERSE == "snp500":
        initial_tickers = get_sp500_tickers()
    elif STOCK_UNIVERSE == "qqq":
        initial_tickers = get_qqq_tickers()
    else:
        print(f"Error: Unknown STOCK_UNIVERSE '{STOCK_UNIVERSE}' in config.py.")
        initial_tickers = []
    
    # --- Step 2: Conditionally run the screener ---
    if ENABLE_SCREENER and initial_tickers:
        # If screener is on, the final list of tickers is the "hot list"
        final_tickers = screen_tickers(polygon_client, initial_tickers)
    else:
        # Otherwise, use the full list from the selected universe
        final_tickers = initial_tickers
    
    # --- Step 3: Fetch detailed data for the FINAL list of tickers ---
    if final_tickers:
        # Use a suffix for filenames if the screener is enabled
        file_suffix = "_screened" if ENABLE_SCREENER else ""
        price_filepath = os.path.join(DATA_DIRECTORY, f"{STOCK_UNIVERSE}{file_suffix}_price_data.json")
        news_filepath = os.path.join(DATA_DIRECTORY, f"{STOCK_UNIVERSE}{file_suffix}_raw_news.json")
        
        print(f"\n--- Starting Detailed Data Collection for {len(final_tickers)} Tickers ---")

        price_data = fetch_price_data(polygon_client, final_tickers, "2021-10-01", "2023-12-31")
        if price_data:
            print(f"\nSaving price data to {price_filepath}...")
            with open(price_filepath, 'w') as f:
                json.dump(price_data, f, indent=4)
            print("Price data saved successfully.")

        news_data = fetch_news_data(polygon_client, final_tickers)
        if news_data:
            print(f"\nSaving news data to {news_filepath}...")
            with open(news_filepath, 'w') as f:
                json.dump(news_data, f, indent=4)
            print("News data saved successfully.")
    else:
        print("\nNo tickers to process after screening.")
    
    print("\nData collection complete.")
    if final_tickers:
        print(f"\nProcess finished successfully for {len(final_tickers)} tickers.")
        print(f"Created files: {price_filepath} and {news_filepath}")
    else:
        print("\nProcess finished. No tickers were selected for data collection.")