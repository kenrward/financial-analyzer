# data_collection.py

import os
import time
import requests
import pandas as pd
from polygon import RESTClient
from datetime import date
import json

# --- Import configuration from config.py ---
try:
    from config import POLYGON_API_KEY, DATA_DIRECTORY, STOCK_UNIVERSE
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
    """Fetches news articles for a list of tickers from Polygon.io."""
    all_news_data = []
    print(f"\nFetching news data for {len(tickers)} tickers...")
    for i, ticker in enumerate(tickers):
        try:
            news_items = client.list_ticker_news(ticker, limit=25)
            for news in news_items:
                all_news_data.append({
                    "ticker": ticker,
                    "published_utc": news.published_utc,
                    "title": news.title,
                    "summary": getattr(news, 'description', 'No summary available.')
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

    if STOCK_UNIVERSE == "snp500":
        tickers = get_sp500_tickers()
    elif STOCK_UNIVERSE == "qqq":
        tickers = get_qqq_tickers()
    else:
        print(f"Error: Unknown STOCK_UNIVERSE '{STOCK_UNIVERSE}' in config.py. Please use 'snp500' or 'qqq'.")
        tickers = []

    if tickers:
        price_filepath = os.path.join(DATA_DIRECTORY, f"{STOCK_UNIVERSE}_price_data.json")
        news_filepath = os.path.join(DATA_DIRECTORY, f"{STOCK_UNIVERSE}_raw_news.json")
        
        print(f"\n--- Starting Data Collection for: {STOCK_UNIVERSE.upper()} ---")

        price_data = fetch_price_data(polygon_client, tickers, "2021-10-01", "2023-12-31")
        if price_data:
            print(f"\nSaving price data to {price_filepath}...")
            with open(price_filepath, 'w') as f:
                json.dump(price_data, f, indent=4)
            print("Price data saved successfully.")

        news_data = fetch_news_data(polygon_client, tickers)
        if news_data:
            print(f"\nSaving news data to {news_filepath}...")
            with open(news_filepath, 'w') as f:
                json.dump(news_data, f, indent=4)
            print("News data saved successfully.")
    
    print("\nData collection complete.")