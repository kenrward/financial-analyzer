# downloader.py
import os
import pandas as pd
from datetime import date, timedelta
import logging
from polygon import RESTClient # Import the RESTClient for API calls

# --- Configuration ---
API_KEY = os.getenv("POLYGON_API_KEY")
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

# --- ✅ V3: New function to download index data via API ---
def download_and_store_indices(client: RESTClient, tickers: list, start_date: str, end_date: str):
    """
    Downloads historical daily data for a list of indices via the API
    and appends it to the partitioned Parquet store.
    """
    logging.info(f"Starting API download for indices: {tickers} from {start_date} to {end_date}...")

    for ticker in tickers:
        try:
            logging.info(f"Fetching API data for index: {ticker}...")
            aggs = client.list_aggs(ticker, 1, "day", start_date, end_date, limit=50000)
            
            all_aggs_data = [{
                'ticker': ticker,
                'date': date.fromtimestamp(a.timestamp / 1000),
                'open': a.open, 'high': a.high, 'low': a.low,
                'close': a.close, 'volume': a.volume
            } for a in aggs]

            if not all_aggs_data:
                logging.warning(f"No API data found for index {ticker}.")
                continue

            df = pd.DataFrame(all_aggs_data)
            df['year'] = pd.to_datetime(df['date']).dt.year
            df['month'] = pd.to_datetime(df['date']).dt.month
            
            logging.info(f"Downloaded {len(df)} records for {ticker}. Saving to Parquet store...")

            df.to_parquet(
                MASTER_PARQUET_ROOT,
                engine='pyarrow', compression='snappy', index=False,
                partition_cols=['year', 'month'],
                existing_data_behavior='delete_matching'
            )
            logging.info(f"Successfully saved API data for {ticker}.")

        except Exception as e:
            logging.error(f"Could not fetch or process API data for index {ticker}. Error: {e}")

if __name__ == "__main__":
    # --- Instructions for Use ---
    
    # For a daily cron job, you would process the previous day for stocks
    # and also update the indices for the last few days.
    previous_day = date.today() - timedelta(days=1)
    process_daily_flat_file(previous_day)

    # Initialize the Polygon client for API calls
    if API_KEY:
        polygon_client = RESTClient(API_KEY)
        
        # For the initial backfill of index data, run this section.
        # Once backfilled, you can change the start_date for daily updates.
        indices_to_download = ['I:VIX']
        end = date.today()
        start = end - timedelta(days=365 * 1) # Backfill 1 year of VIX data
        
        download_and_store_indices(
            polygon_client,
            indices_to_download,
            start.strftime('%Y-%m-%d'),
            end.strftime('%Y-%m-%d')
        )
    else:
        logging.warning("POLYGON_API_KEY not found, skipping index download.")
