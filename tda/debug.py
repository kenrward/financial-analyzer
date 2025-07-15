# downloader.py (DEBUG version)
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
    Reads a daily Polygon.io flat file and prints its columns for debugging.
    """
    date_str = target_date.strftime('%Y-%m-%d')
    file_path = os.path.join(FLAT_FILE_ROOT_PATH, target_date.strftime('%Y'), target_date.strftime('%m'), f"{date_str}.csv.gz")
    
    logging.info(f"Processing file: {file_path}")

    try:
        df = pd.read_csv(file_path, compression='gzip')
        logging.info(f"Successfully read {len(df)} records from {file_path}.")
        
        # --- ✅ DEBUGGING STEP ---
        # Print the list of actual column names from the file and then exit.
        print("\n\n--- DEBUG INFO ---")
        print(f"Actual columns found in the file: {df.columns.to_list()}")
        print("------------------\n")
        
        # We will stop here for now.
        return

    except FileNotFoundError:
        logging.warning(f"File not found for {date_str}. Skipping.")
    except Exception as e:
        logging.error(f"Failed to process data for {date_str}. Error: {e}", exc_info=True)


if __name__ == "__main__":
    previous_day = date.today() - timedelta(days=1)
    process_daily_flat_file(previous_day)