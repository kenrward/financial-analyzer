# downloader.py
import os
import pandas as pd
from datetime import date, timedelta
import logging

# --- Configuration ---
FLAT_FILE_ROOT_PATH = "/mnt/shared-drive/polygon_data/us_stocks_sip/day_aggs_v1"
MASTER_PARQUET_PATH = "/mnt/shared-drive/us_stocks_daily.parquet"

# --- Setup Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("downloader.log"),
        logging.StreamHandler()
    ]
)

def process_daily_flat_file(target_date: date):
    """
    Reads a daily Polygon.io flat file from a local path, processes it,
    and appends the data to the master Parquet data store.
    """
    date_str = target_date.strftime('%Y-%m-%d')
    file_path = os.path.join(FLAT_FILE_ROOT_PATH, target_date.strftime('%Y'), target_date.strftime('%m'), f"{date_str}.csv.gz")
    
    logging.info(f"Processing file: {file_path}")

    try:
        df = pd.read_csv(file_path, compression='gzip')
        logging.info(f"Successfully read {len(df)} records from {file_path}.")
        
        # --- ✅ Final, Correct Data Cleaning & Formatting ---
        # Convert the 'window_start' Unix timestamp (in nanoseconds) to a date
        df['date'] = pd.to_datetime(df['window_start'], unit='ns').dt.date
        
        # Select and rename columns for our master database
        # The flat file uses 'ticker', 'volume', 'open', 'close', 'high', 'low'
        final_df = df[['date', 'ticker', 'open', 'high', 'low', 'close', 'volume']]

        # --- Store the data ---
        if os.path.exists(MASTER_PARQUET_PATH):
            logging.info(f"Appending data to existing file: {MASTER_PARQUET_PATH}")
            existing_df = pd.read_parquet(MASTER_PARQUET_PATH)
            # Combine, remove duplicates for the date being processed, and save
            combined_df = pd.concat([existing_df[existing_df['date'] != pd.to_datetime(target_date).date()], final_df])
            combined_df.to_parquet(MASTER_PARQUET_PATH, engine='pyarrow', compression='snappy', index=False)
        else:
            logging.info(f"Creating new data file: {MASTER_PARQUET_PATH}")
            os.makedirs(os.path.dirname(MASTER_PARQUET_PATH), exist_ok=True)
            final_df.to_parquet(MASTER_PARQUET_PATH, engine='pyarrow', compression='snappy', index=False)
            
        logging.info(f"Successfully processed and saved data for {date_str}.")

    except FileNotFoundError:
        logging.warning(f"File not found for {date_str} (likely a weekend or market holiday). Skipping.")
    except Exception as e:
        logging.error(f"Failed to process data for {date_str}. Error: {e}", exc_info=True)


if __name__ == "__main__":
    # --- Instructions for Use ---
    
    # To run a daily update for the previous day, use this:
    previous_day = date.today() - timedelta(days=1)
    process_daily_flat_file(previous_day)

    # To build your database initially for the last 3 years,
    # you can uncomment and run this loop:
    #
    # logging.info("Starting initial backfill for the last 3 years...")
    # for i in range(1, 365 * 3):
    #     target_day = date.today() - timedelta(days=i)
    #     process_daily_flat_file(target_day)
    # logging.info("Initial backfill complete.")