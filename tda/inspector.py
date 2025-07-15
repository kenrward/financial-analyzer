# inspector.py
import pandas as pd
import os

# The path to your master Parquet file database
MASTER_PARQUET_PATH = "/mnt/shared-drive/us_stocks_daily.parquet"

def inspect_parquet_file():
    """Reads the master Parquet file and prints a summary of its contents."""
    
    if not os.path.exists(MASTER_PARQUET_PATH):
        print(f"Error: File not found at '{MASTER_PARQUET_PATH}'")
        return

    try:
        print(f"Reading data from {MASTER_PARQUET_PATH}...")
        df = pd.read_parquet(MASTER_PARQUET_PATH)
        
        if df.empty:
            print("The Parquet file is empty.")
            return

        # --- Print Summary Information ---
        print("\n--- 📊 DATA SUMMARY ---")
        print(f"Total Records: {len(df)}")
        
        df['date'] = pd.to_datetime(df['date'])
        min_date = df['date'].min().strftime('%Y-%m-%d')
        max_date = df['date'].max().strftime('%Y-%m-%d')
        print(f"Date Range: {min_date} to {max_date}")

        # --- ✅ CORRECTED TICKER HANDLING ---
        # Get unique tickers from the 'ticker' column
        unique_tickers_raw = df['ticker'].unique()
        # Filter out any None or NaN values before sorting
        clean_tickers = sorted([t for t in unique_tickers_raw if pd.notna(t)])
        
        print(f"Tickers in File ({len(clean_tickers)}): {clean_tickers}")
        
        print("\n--- First 5 Rows ---")
        print(df.head())
        print("--------------------")

    except Exception as e:
        print(f"An error occurred while reading the file: {e}")

if __name__ == "__main__":
    inspect_parquet_file()