# data_collection.py

import os
import time
import requests
import pandas as pd
from polygon import RESTClient
from datetime import date
import json # <-- ADDED: Import the correct library

# --- Import configuration from config.py ---
try:
    from config import POLYGON_API_KEY, DATA_DIRECTORY
except ImportError:
    print("Error: config.py not found. Please create it with your POLYGON_API_KEY and DATA_DIRECTORY.")
    exit()

def get_sp500_tickers():
    """
    Scrapes the S&P 500 tickers from the Wikipedia page.

    Returns:
        list: A list of S&P 500 ticker symbols.
    """
    print("Fetching S&P 500 tickers from Wikipedia...")
    try:
        url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
        tables = pd.read_html(url)
        sp500_df = tables[0]
        tickers = sp500_df['Symbol'].tolist()
        print(f"Successfully fetched {len(tickers)} S&P 500 tickers.")
        tickers = [ticker.replace('.', '-') for ticker in tickers]
        return tickers
    except Exception as e:
        print(f"Error fetching S&P 500 tickers: {e}")
        return []

def fetch_price_data(client, tickers, start_date, end_date):
    """
    Fetches daily open/close prices for a list of tickers from Polygon.io.

    Args:
        client (RESTClient): The Polygon.io REST client.
        tickers (list): A list of stock tickers.
        start_date (str): The start date in 'YYYY-MM-DD' format.
        end_date (str): The end date in 'YYYY-MM-DD' format.

    Returns:
        dict: A dictionary containing price data for each ticker.
    """
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
    Fetches news articles for a list of tickers from Polygon.io.

    Args:
        client (RESTClient): The Polygon.io REST client.
        tickers (list): A list of stock tickers.

    Returns:
        list: A list of dictionaries, each containing news article details.
    """
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
        print(f"Creating data directory: {DATA_DIRECTORY}")
        os.makedirs(DATA_DIRECTORY)

    polygon_client = RESTClient(POLYGON_API_KEY)

    sp500_tickers = get_sp500_tickers()
    
    # For development, you might want to test with a small subset of tickers
    # sp500_tickers = sp500_tickers[:10]

    if sp500_tickers:
        price_data = fetch_price_data(polygon_client, sp500_tickers, "2021-10-01", "2023-12-31")
        if price_data:
            price_filepath = os.path.join(DATA_DIRECTORY, "sp500_price_data.json")
            print(f"\nSaving price data to {price_filepath}...")
            with open(price_filepath, 'w') as f:
                # --- CORRECTED LINE ---
                json.dump(price_data, f, indent=4)
            print("Price data saved successfully.")

        news_data = fetch_news_data(polygon_client, sp500_tickers)
        if news_data:
            news_filepath = os.path.join(DATA_DIRECTORY, "sp500_raw_news.json")
            print(f"\nSaving news data to {news_filepath}...")
            with open(news_filepath, 'w') as f:
                # --- CORRECTED LINE ---
                json.dump(news_data, f, indent=4)
            print("News data saved successfully.")
    
    print("\nData collection complete.")