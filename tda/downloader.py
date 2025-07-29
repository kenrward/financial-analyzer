# downloader.py
import os
import pandas as pd
from datetime import date, timedelta
import logging
import yfinance as yf # Import the yfinance library for index data

# --- Configuration ---
# API_KEY is no longer needed for this script if you only download indices via yfinance
# API_KEY = os.getenv("POLYGON_API_KEY") 
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

def process_daily_flat_file(target_date: date):
    """
    Reads a daily stock flat file and saves it to the partitioned Parquet store.
    """
    year = target_date.strftime('%Y')
    month = target_date.strftime('%m')
    date_str = target_date.strftime('%Y-%m-%d')
    
    source_file_path = os.path.join(FLAT_FILE_ROOT_PATH, year, month, f"{date_str}.csv.gz")
    logging.info(f"Processing stock flat file: {source_file_path}")

    try:
        df = pd.read_csv(source_file_path, compression='gzip')
        df['date'] = pd.to_datetime(df['window_start'], unit='ns').dt.date
        df.rename(columns={'from': 'ticker'}, inplace=True)
        df['year'] = pd.to_datetime(df['date']).dt.year
        df['month'] = pd.to_datetime(df['date']).dt.month
        final_df = df[['date', 'year', 'month', 'ticker', 'open', 'high', 'low', 'close', 'volume']]

        final_df.to_parquet(
            MASTER_PARQUET_ROOT,
            engine='pyarrow', compression='snappy', index=False,
            partition_cols=['year', 'month'],
            existing_data_behavior='delete_matching'
        )
        logging.info(f"Successfully processed and saved stock data for {date_str}.")
    except FileNotFoundError:
        logging.warning(f"Stock flat file not found for {date_str} (likely a holiday). Skipping.")
    except Exception as e:
        logging.error(f"Failed to process stock flat file for {date_str}. Error: {e}", exc_info=True)

# --- ✅ V3: Updated function to download index data via yfinance ---
def download_and_store_indices_yfinance(tickers: list, start_date: str, end_date: str):
    """
    Downloads historical daily data for a list of indices via yfinance
    and merges it into the partitioned Parquet store.
    """
    logging.info(f"Starting yfinance download for indices: {tickers} from {start_date} to {end_date}...")

    try:
        # Download all requested index data in one call
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
            logging.warning("No valid dataframes to process after download.")
            return
            
        final_df = pd.concat(all_data)
        final_df.reset_index(inplace=True)
        final_df.rename(columns={'Date': 'date', 'Open': 'open', 'High': 'high', 'Low': 'low', 'Close': 'close', 'Volume': 'volume'}, inplace=True)
        final_df['date'] = pd.to_datetime(final_df['date']).dt.date
        final_df['year'] = pd.to_datetime(final_df['date']).dt.year
        final_df['month'] = pd.to_datetime(final_df['date']).dt.month
        
        logging.info(f"Downloaded {len(final_df)} total index records. Merging into Parquet store...")

        # Group data by month to process one monthly file at a time
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
    # For a daily cron job, you would process the previous day's stock flat file
    previous_day = date.today() - timedelta(days=1)
    process_daily_flat_file(previous_day)

    # Then, update the indices for the last few days using yfinance
    # The ticker for VIX in yfinance is '^VIX'
    indices_to_download = ['^VIX'] 
    end = date.today()
    # For daily updates, you only need a few days to catch up
    start = end - timedelta(days=5) 
    
    # For the initial backfill, use a wider date range
    #  start = end - timedelta(days=365 * 3) # Backfill 3 years of VIX data
    
    download_and_store_indices_yfinance(
        indices_to_download,
        start.strftime('%Y-%m-%d'),
        end.strftime('%Y-%m-%d')
    )
