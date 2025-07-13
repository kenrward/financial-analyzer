# downloader.py
import os
import pandas as pd
from datetime import date, timedelta
from polygon import RESTClient
import logging

# --- Configuration ---
API_KEY = os.getenv("POLYGON_API_KEY")
STORAGE_PATH = "/mnt/shared-drive/us_stocks_daily.parquet"

# --- Setup Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("downloader.log"),
        logging.StreamHandler()
    ]
)

def download_and_store_equities(client: RESTClient, tickers: list, start_date: str, end_date: str):
    """
    Downloads historical daily data for a list of tickers over a date range
    and appends it to a local Parquet file.
    """
    all_aggs = []
    logging.info(f"Starting download for {len(tickers)} tickers from {start_date} to {end_date}...")

    for ticker in tickers:
        try:
            logging.info(f"Fetching data for {ticker}...")
            # Using the list_aggs method for robust date range queries
            aggs = client.list_aggs(
                ticker,
                1,
                "day",
                start_date,
                end_date,
                limit=50000 # Max limit
            )
            
            # Convert generator to a list of dictionaries and add the ticker
            for a in aggs:
                all_aggs.append({
                    'ticker': ticker,
                    'date': date.fromtimestamp(a.timestamp / 1000),
                    'open': a.open,
                    'high': a.high,
                    'low': a.low,
                    'close': a.close,
                    'volume': a.volume
                })
        except Exception as e:
            logging.error(f"Could not fetch data for {ticker}. Error: {e}")

    if not all_aggs:
        logging.warning("No data was downloaded. Exiting.")
        return

    df = pd.DataFrame(all_aggs)
    logging.info(f"Successfully downloaded {len(df)} total records.")

    # --- Store the data ---
    if os.path.exists(STORAGE_PATH):
        logging.info(f"Appending data to existing file: {STORAGE_PATH}")
        existing_df = pd.read_parquet(STORAGE_PATH)
        # Combine new and old data, then remove any duplicates
        combined_df = pd.concat([existing_df, df]).drop_duplicates(subset=['date', 'ticker'], keep='last')
        combined_df.to_parquet(STORAGE_PATH, engine='pyarrow', compression='snappy', index=False)
    else:
        logging.info(f"Creating new data file: {STORAGE_PATH}")
        os.makedirs(os.path.dirname(STORAGE_PATH), exist_ok=True)
        df.to_parquet(STORAGE_PATH, engine='pyarrow', compression='snappy', index=False)
        
    logging.info(f"Successfully saved data to {STORAGE_PATH}.")


if __name__ == "__main__":
    if not API_KEY:
        raise ValueError("POLYGON_API_KEY environment variable not set.")
    
    polygon_client = RESTClient(API_KEY)

    # --- Define tickers and date range for download ---
    # List of important tickers to bootstrap your database
    TICKERS_TO_DOWNLOAD = ['SPY', 'QQQ', '^VIX'] 
    
    # For the initial bulk download, use a wide date range
    end_date = date.today()
    start_date = end_date - timedelta(days=365 * 3) # 3 years of data

    # For daily updates (in a cron job), you would use:
    # end_date = date.today()
    # start_date = end_date - timedelta(days=3) # Get last few days to be safe

    download_and_store_equities(
        polygon_client,
        TICKERS_TO_DOWNLOAD,
        start_date.strftime('%Y-%m-%d'),
        end_date.strftime('%Y-%m-%d')
    )