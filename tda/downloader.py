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

def download_and_store_equities(client: RESTClient, target_date: date):
    """
    Downloads the daily stock aggregates for a specific date using the REST API,
    converts it to Parquet, and appends it to the local data store.
    """
    target_date_str = target_date.strftime('%Y-%m-%d')
    logging.info(f"Starting equities download for date: {target_date_str}")
    
    try:
        # --- ✅ THE FIX ---
        # Using the get_grouped_daily_aggs method which is confirmed to work.
        daily_aggs = client.get_grouped_daily_aggs(target_date_str, adjusted=True)
        
        # This endpoint returns a list of Aggregate objects
        df = pd.DataFrame([a.__dict__ for a in daily_aggs])
        
        if df.empty:
            logging.warning(f"No equity data found for {target_date_str} (likely a market holiday).")
            return

        logging.info(f"Successfully downloaded {len(df)} equity records for {target_date_str}.")
        
        # --- Data Cleaning & Formatting ---
        # Note: The attribute names are already lowercase and match our desired format
        # We just need to rename 'T' from the REST API object model if it exists
        df.rename(columns={'T': 'ticker', 'v': 'volume', 'o': 'open', 'h': 'high', 'l': 'low', 'c': 'close', 't':'timestamp'}, inplace=True)
        # Convert Unix timestamp (in milliseconds) to a proper date
        df['date'] = pd.to_datetime(df['timestamp'], unit='ms').dt.date

        # Select and reorder columns to match our desired schema
        final_df = df[['date', 'ticker', 'open', 'high', 'low', 'close', 'volume']]
        
        # --- Store the data ---
        if os.path.exists(STORAGE_PATH):
            logging.info(f"Appending data to existing file: {STORAGE_PATH}")
            existing_df = pd.read_parquet(STORAGE_PATH)
            combined_df = pd.concat([existing_df, final_df]).drop_duplicates(subset=['date', 'ticker'], keep='last')
            combined_df.to_parquet(STORAGE_PATH, engine='pyarrow', compression='snappy', index=False)
        else:
            logging.info(f"Creating new data file: {STORAGE_PATH}")
            os.makedirs(os.path.dirname(STORAGE_PATH), exist_ok=True)
            final_df.to_parquet(STORAGE_PATH, engine='pyarrow', compression='snappy', index=False)
            
        logging.info(f"Successfully saved equity data for {target_date_str}.")

    except Exception as e:
        logging.error(f"Failed to process equity data for {target_date_str}. Error: {e}", exc_info=True)


if __name__ == "__main__":
    if not API_KEY:
        raise ValueError("POLYGON_API_KEY environment variable not set.")
    
    polygon_client = RESTClient(API_KEY)
    previous_day = date.today() - timedelta(days=1)
    
    download_and_store_equities(polygon_client, previous_day)