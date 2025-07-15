# downloader.py
import os
import pandas as pd
from datetime import date, timedelta
import logging

# --- Configuration ---
# The root path where your daily flat files are stored
FLAT_FILE_ROOT_PATH = "/mnt/shared-drive/polygon_data/us_stocks_sip/day_aggs_v1"

# The path to your master Parquet file database
MASTER_PARQUET_PATH = "/mnt/shared-drive/trading_agent_data/us_stocks_daily.parquet"

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
    year = target_date.strftime('%Y')
    month = target_date.strftime('%m')
    date_str = target_date.strftime('%Y-%m-%d')
    
    # Construct the full path to the daily gzipped CSV file
    file_path = os.path.join(FLAT_FILE_ROOT_PATH, year, month, f"{date_str}.csv.gz")
    
    logging.info(f"Processing file: {file_path}")

    try:
        df = pd.read_csv(file_path, compression='gzip')
        logging.info(f"Successfully read {len(df)} records from {file_path}.")
        
        # --- ✅ Simplified Data Cleaning & Formatting ---
        # Convert Unix timestamp (in nanoseconds for flat files) to a proper date
        df['date'] = pd.to_datetime(df['timestamp'], unit='ns').dt.date

        # Select and rename columns in one step for clarity
        final_df = df.rename(columns={'from': 'ticker'}) # The ticker column in daily aggs is named 'from'
        final_df = final_df[['date', 'ticker', 'open', 'high', 'low', 'close', 'volume']]

        # --- Store the data ---
        if os.path.exists(MASTER_PARQUET_PATH):
            logging.info(f"Appending data to existing file: {MASTER_PARQUET_PATH}")
            existing_df = pd.read_parquet(MASTER_PARQUET_PATH)
            combined_df = pd.concat([existing_df, final_df]).drop_duplicates(subset=['date', 'ticker'], keep='last')
            combined_df.to_parquet(MASTER_PARQUET_PATH, engine='pyarrow', compression='snappy', index=False)
        else:
            logging.info(f"Creating new data file: {MASTER_PARQUET_PATH}")
            os.makedirs(os.path.dirname(MASTER_PARQUET_PATH), exist_ok=True)
            final_df.to_parquet(MASTER_PARQUET_PATH, engine='pyarrow', compression='snappy', index=False)
            
        logging.info(f"Successfully processed and saved data for {date_str}.")

    except FileNotFoundError:
        logging.warning(f"File not found for {date_str} (likely a weekend or holiday). Skipping.")
    except Exception as e:
        logging.error(f"Failed to process data for {date_str}. Error: {e}", exc_info=True)


if __name__ == "__main__":
    # To build your database initially, you can loop through a range of dates
    # For example, to process all of June 2025:
    # start = date(2025, 6, 1)
    # end = date(2025, 6, 30)
    # current_date = start
    # while current_date <= end:
    #     process_daily_flat_file(current_date)
    #     current_date += timedelta(days=1)

    # For a daily cron job, you would just process the previous day
    previous_day = date.today() - timedelta(days=15)
    process_daily_flat_file(previous_day)