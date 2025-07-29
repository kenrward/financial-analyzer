# downloader.py
import os
import pandas as pd
from datetime import date, timedelta
import logging

# --- Configuration ---
FLAT_FILE_ROOT_PATH = "/mnt/shared-drive/polygon_data/us_stocks_sip/day_aggs_v1"
# The new root directory for our partitioned database
MASTER_PARQUET_ROOT = "/mnt/shared-drive/us_stocks_daily_partitioned"

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
    Reads a daily Polygon.io flat file, processes it, and saves it to a
    partitioned Parquet data store.
    """
    year = target_date.strftime('%Y')
    month = target_date.strftime('%m')
    date_str = target_date.strftime('%Y-%m-%d')
    
    file_path = os.path.join(FLAT_FILE_ROOT_PATH, year, month, f"{date_str}.csv.gz")
    
    logging.info(f"Processing file: {file_path}")

    try:
        df = pd.read_csv(file_path, compression='gzip')
        logging.info(f"Successfully read {len(df)} records from {file_path}.")
        
        # --- Data Cleaning & Formatting ---
        df['date'] = pd.to_datetime(df['window_start'], unit='ns').dt.date
        df.rename(columns={'from': 'ticker'}, inplace=True)
        
        # Add year and month columns for partitioning
        df['year'] = pd.to_datetime(df['date']).dt.year
        df['month'] = pd.to_datetime(df['date']).dt.month

        final_df = df[['date', 'year', 'month', 'ticker', 'open', 'high', 'low', 'close', 'volume']]

        # --- Store the data using partitioning ---
        # This is much more efficient than reading/writing one giant file.
        # It will create the year=/month= subdirectories automatically.
        final_df.to_parquet(
            MASTER_PARQUET_ROOT,
            engine='pyarrow',
            compression='snappy',
            index=False,
            partition_cols=['year', 'month'],
            existing_data_behavior='delete_matching' # Overwrites partitions for the same year/month
        )
            
        logging.info(f"Successfully processed and saved data for {date_str}.")

    except FileNotFoundError:
        logging.warning(f"File not found for {date_str} (likely a weekend or market holiday). Skipping.")
    except Exception as e:
        logging.error(f"Failed to process data for {date_str}. Error: {e}", exc_info=True)


if __name__ == "__main__":
    # --- Instructions for Use ---
    
    # For a daily cron job, you should process the previous day
    previous_day = date.today() - timedelta(days=1)
    process_daily_flat_file(previous_day)

    # To build your database initially for the last 3 years,
    # you can uncomment and run this loop.
    # IMPORTANT: You should delete your old, single Parquet file before running this.
    
    # logging.info("Starting initial backfill for the last 3 years...")
    # for i in range(1, 365 * 3):
    #     target_day = date.today() - timedelta(days=i)
    #     process_daily_flat_file(target_day)
    # logging.info("Initial backfill complete.")
