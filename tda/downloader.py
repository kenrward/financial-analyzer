# downloader.py
import os
import pandas as pd
from datetime import date, timedelta
from polygon import RESTClient
import logging

# --- Configuration ---
API_KEY = os.getenv("POLYGON_API_KEY")

# Define where to store your local data files
STOCKS_STORAGE_PATH = "/mnt/shared-drive/us_stocks_daily.parquet"
# You can add a path for options data later when needed
# OPTIONS_STORAGE_PATH = "/opt/trading_agent_data/us_options_daily.parquet"

# --- Setup Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("downloader.log"),
        logging.StreamHandler()
    ]
)

def download_and_store_equities(client: RESTClient, target_date: date):
    """
    Downloads the daily stock aggregates file for a specific date,
    converts it to Parquet, and appends it to the local data store.
    """
    target_date_str = target_date.strftime('%Y-%m-%d')
    logging.info(f"Starting equities download for date: {target_date_str}")
    
    try:
        # Download the daily OHLCV flat file (this returns a generator of dicts)
        daily_aggs_stream = client.download_daily_open_close_aggs(target_date_str)
        
        df = pd.DataFrame(daily_aggs_stream)
        
        if df.empty:
            logging.warning(f"No equity data found for {target_date_str} (likely a market holiday).")
            return

        logging.info(f"Successfully downloaded {len(df)} equity records for {target_date_str}.")
        
        # --- Data Cleaning & Formatting ---
        df.rename(columns={'timestamp': 'date', 'ticker': 'T', 'open': 'o', 'high': 'h', 'low': 'l', 'close': 'c', 'volume': 'v'}, inplace=True)
        df['date'] = pd.to_datetime(df['date'], unit='ms').dt.date

        # --- Store the data ---
        if os.path.exists(STOCKS_STORAGE_PATH):
            logging.info(f"Appending data to existing file: {STOCKS_STORAGE_PATH}")
            existing_df = pd.read_parquet(STOCKS_STORAGE_PATH)
            combined_df = pd.concat([existing_df, df]).drop_duplicates(subset=['date', 'T'], keep='last')
            combined_df.to_parquet(STOCKS_STORAGE_PATH, engine='pyarrow', compression='snappy', index=False)
        else:
            logging.info(f"Creating new data file: {STOCKS_STORAGE_PATH}")
            os.makedirs(os.path.dirname(STOCKS_STORAGE_PATH), exist_ok=True)
            df.to_parquet(STOCKS_STORAGE_PATH, engine='pyarrow', compression='snappy', index=False)
            
        logging.info(f"Successfully saved equity data for {target_date_str}.")

    except Exception as e:
        # Polygon's client may raise an error for dates with no data (holidays/weekends)
        logging.error(f"Failed to process equity data for {target_date_str}. Error: {e}")

# You can add a similar function for options data when ready
# def download_and_store_options(client: RESTClient, target_date: date):
#     logging.info("Options downloader not yet implemented.")


if __name__ == "__main__":
    if not API_KEY:
        raise ValueError("POLYGON_API_KEY environment variable not set.")
    
    # Initialize the client
    polygon_client = RESTClient(API_KEY)

    # This script is intended to be run daily to get the previous trading day's data.
    # Polygon files are typically available by the next morning.
    previous_day = date.today() - timedelta(days=1)
    
    download_and_store_equities(polygon_client, previous_day)
    # download_and_store_options(polygon_client, previous_day)