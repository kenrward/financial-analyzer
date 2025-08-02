# downloader.py
import os
import pandas as pd
from datetime import date, timedelta
import logging
import yfinance as yf
import subprocess
import sys

# --- Configuration ---
FLAT_FILE_ROOT_PATH = "/mnt/shared-drive/polygon_data/us_stocks_sip/day_aggs_v1"
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

def run_s3_downloader():
    """Runs the s3_downloader.py script as a subprocess."""
    logging.info("--- Kicking off S3 Downloader to sync flat files ---")
    # Assumes s3_downloader.py is in the same directory
    script_path = os.path.join(os.path.dirname(__file__), "s3_downloader.py")
    if not os.path.exists(script_path):
        logging.error(f"s3_downloader.py not found at {script_path}. Skipping S3 sync.")
        return

    try:
        # Use the same python executable that is running this script
        result = subprocess.run([sys.executable, script_path], check=True, capture_output=True, text=True)
        logging.info("S3 Downloader finished successfully.")
        logging.info(result.stdout)
    except subprocess.CalledProcessError as e:
        logging.error("--- S3 Downloader FAILED ---")
        logging.error(f"Return code: {e.returncode}")
        logging.error(f"Output: {e.stdout}")
        logging.error(f"Error Output: {e.stderr}")
    except Exception as e:
        logging.error(f"An unexpected error occurred while running s3_downloader.py: {e}")

def process_daily_flat_file(target_date: date):
    """
    Reads a daily stock flat file and appends its data to the correct
    monthly Parquet file in a YYYY/MM.parquet structure.
    """
    year = target_date.strftime('%Y')
    month_str = target_date.strftime('%m')
    date_str = target_date.strftime('%Y-%m-%d')
    
    source_file_path = os.path.join(FLAT_FILE_ROOT_PATH, year, month_str, f"{date_str}.csv.gz")
    logging.info(f"Processing stock flat file: {source_file_path}")

    try:
        df = pd.read_csv(source_file_path, compression='gzip')
        df['date'] = pd.to_datetime(df['window_start'], unit='ns').dt.date
        df.rename(columns={'from': 'ticker'}, inplace=True)
        final_df = df[['date', 'ticker', 'open', 'high', 'low', 'close', 'volume']]

        month_dir = os.path.join(MASTER_PARQUET_ROOT, year)
        os.makedirs(month_dir, exist_ok=True)
        month_parquet_path = os.path.join(month_dir, f"{month_str}.parquet")

        if os.path.exists(month_parquet_path):
            logging.info(f"Appending data to existing monthly file: {month_parquet_path}")
            existing_df = pd.read_parquet(month_parquet_path)
            combined_df = pd.concat([existing_df[existing_df['date'] != pd.to_datetime(target_date).date()], final_df])
            combined_df.to_parquet(month_parquet_path, engine='pyarrow', compression='snappy', index=False)
        else:
            logging.info(f"Creating new monthly file: {month_parquet_path}")
            final_df.to_parquet(month_parquet_path, engine='pyarrow', compression='snappy', index=False)
            
        logging.info(f"Successfully processed and saved stock data for {date_str}.")

    except FileNotFoundError:
        logging.warning(f"Stock flat file not found for {date_str} (likely a holiday). Skipping.")
    except Exception as e:
        logging.error(f"Failed to process stock flat file for {date_str}. Error: {e}", exc_info=True)

def download_and_store_indices_yfinance(tickers: list, start_date: str, end_date: str):
    """
    Downloads historical daily data for indices via yfinance and merges it
    into the monthly partitioned Parquet store.
    """
    logging.info(f"Starting yfinance download for indices: {tickers} from {start_date} to {end_date}...")

    try:
        df = yf.download(tickers, start=start_date, end=end_date, group_by='ticker')
        if df.empty:
            logging.warning(f"No data returned from yfinance for tickers: {tickers}")
            return

        all_data = []
        for ticker in tickers:
            if ticker in df.columns:
                symbol_df = df[ticker].copy()
                symbol_df.dropna(inplace=True)
                symbol_df['ticker'] = ticker
                all_data.append(symbol_df)

        if not all_data:
            return
            
        final_df = pd.concat(all_data)
        final_df.reset_index(inplace=True)
        final_df.rename(columns={'Date': 'date', 'Open': 'open', 'High': 'high', 'Low': 'low', 'Close': 'close', 'Volume': 'volume'}, inplace=True)
        
        final_df['date'] = pd.to_datetime(final_df['date'])
        final_df['year'] = final_df['date'].dt.year
        final_df['month'] = final_df['date'].dt.month
        final_df['date'] = final_df['date'].dt.date
        
        logging.info(f"Downloaded {len(final_df)} total index records. Merging into Parquet store...")

        for (year, month), group in final_df.groupby(['year', 'month']):
            month_dir = os.path.join(MASTER_PARQUET_ROOT, str(year))
            os.makedirs(month_dir, exist_ok=True)
            month_parquet_path = os.path.join(month_dir, f"{str(month).zfill(2)}.parquet")

            if os.path.exists(month_parquet_path):
                existing_df = pd.read_parquet(month_parquet_path)
                combined_df = pd.concat([existing_df, group]).drop_duplicates(subset=['date', 'ticker'], keep='last')
                combined_df.to_parquet(month_parquet_path, engine='pyarrow', compression='snappy', index=False)
                logging.info(f"Merged index data into {month_parquet_path}")
            else:
                group.to_parquet(month_parquet_path, engine='pyarrow', compression='snappy', index=False)
                logging.info(f"Created new monthly file for indices: {month_parquet_path}")

        logging.info(f"Successfully saved all index data.")
    except Exception as e:
        logging.error(f"Could not fetch or process index data from yfinance. Error: {e}")

if __name__ == "__main__":
    # Step 1: Run the S3 downloader to ensure raw files are up to date
    run_s3_downloader()

    # Step 2: Process the flat file for the previous day
    previous_day = date.today() - timedelta(days=1)
    process_daily_flat_file(previous_day)
    
    logging.info("Daily flat file processing complete.")

    # Step 3: Update the index data
    indices_to_download = ['^VIX'] 
    end = date.today()
    start = end - timedelta(days=5) # Get last 5 days of data to be safe
    
    download_and_store_indices_yfinance(
        indices_to_download,
        start.strftime('%Y-%m-%d'),
        end.strftime('%Y-%m-%d')
    )
