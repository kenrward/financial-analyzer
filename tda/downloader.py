# downloader.py
import os
import pandas as pd
from datetime import date, timedelta
import logging

# --- Configuration ---
FLAT_FILE_ROOT_PATH = "/mnt/shared-drive/polygon_data/us_stocks_sip/day_aggs_v1"
# The new root directory for our monthly Parquet files
MASTER_PARQUET_ROOT = "/mnt/shared-drive/us_stocks_daily_by_month"

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
    Reads a daily Polygon.io flat file and appends its data to the correct
    monthly Parquet file in a YYYY/MM.parquet structure.
    """
    year = target_date.strftime('%Y')
    month = target_date.strftime('%m')
    date_str = target_date.strftime('%Y-%m-%d')
    
    # Construct the path to the source gzipped CSV file
    source_file_path = os.path.join(FLAT_FILE_ROOT_PATH, year, month, f"{date_str}.csv.gz")
    
    logging.info(f"Processing file: {source_file_path}")

    try:
        df = pd.read_csv(source_file_path, compression='gzip')
        logging.info(f"Successfully read {len(df)} records from {source_file_path}.")
        
        # --- Data Cleaning & Formatting ---
        df['date'] = pd.to_datetime(df['window_start'], unit='ns').dt.date
        df.rename(columns={'from': 'ticker'}, inplace=True)
        final_df = df[['date', 'ticker', 'open', 'high', 'low', 'close', 'volume']]

        # --- Store the data in the correct monthly file ---
        month_dir = os.path.join(MASTER_PARQUET_ROOT, year)
        os.makedirs(month_dir, exist_ok=True)
        month_parquet_path = os.path.join(month_dir, f"{month}.parquet")

        if os.path.exists(month_parquet_path):
            logging.info(f"Appending data to existing monthly file: {month_parquet_path}")
            existing_df = pd.read_parquet(month_parquet_path)
            # Combine, remove duplicates for the specific day, and save
            combined_df = pd.concat([existing_df[existing_df['date'] != pd.to_datetime(target_date).date()], final_df])
            combined_df.to_parquet(month_parquet_path, engine='pyarrow', compression='snappy', index=False)
        else:
            logging.info(f"Creating new monthly file: {month_parquet_path}")
            final_df.to_parquet(month_parquet_path, engine='pyarrow', compression='snappy', index=False)
            
        logging.info(f"Successfully processed and saved data for {date_str}.")

    except FileNotFoundError:
        logging.warning(f"File not found for {date_str} (likely a weekend or market holiday). Skipping.")
    except Exception as e:
        logging.error(f"Failed to process data for {date_str}. Error: {e}", exc_info=True)


if __name__ == "__main__":
    # --- Instructions for Use ---

    # To run a daily update for the previous day (for a cron job), use this:
    previous_day = date.today() - timedelta(days=1)
    process_daily_flat_file(previous_day)

    # To build your database initially for the last 3 years,
    # you can uncomment and run this loop.
    # IMPORTANT: You should delete your old data directory before running this.
    
    # logging.info("Starting initial backfill for the last 3 years...")
    # for i in range(1, 365 * 3):
    #     target_day = date.today() - timedelta(days=i)
    #     process_daily_flat_file(target_day)
    # logging.info("Initial backfill complete.")
