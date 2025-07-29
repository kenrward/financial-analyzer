# ta_api.py
import os
import pandas as pd
import numpy as np 
import ta 
from flask import Flask, jsonify, request
import logging
from datetime import date, timedelta

app = Flask(__name__)

# --- Configuration ---
DATA_PATH = "/mnt/shared-drive/us_stocks_daily_by_month"

# --- Setup Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# --- Helper function to read from the new monthly partitioned data ---
def get_data_from_local_store(ticker: str):
    """
    Reads the last 15 months of data for a specific ticker from the
    monthly partitioned Parquet store.
    """
    logging.info(f"Reading last 15 months of data for ticker '{ticker}' from: {DATA_PATH}")
    
    dfs_to_load = []
    
    # Loop through the last 13 months to ensure we have at least one full year of data
    for i in range(15):
        target_date = date.today() - timedelta(days=i * 30)
        year = target_date.strftime('%Y')
        month = target_date.strftime('%m')
        
        file_path = os.path.join(DATA_PATH, year, f"{month}.parquet")
        
        if os.path.exists(file_path):
            try:
                monthly_df = pd.read_parquet(file_path, filters=[('ticker', '==', ticker)])
                if not monthly_df.empty:
                    dfs_to_load.append(monthly_df)
            except Exception as e:
                logging.warning(f"Could not read or filter file {file_path} for ticker {ticker}. Error: {e}")

    if not dfs_to_load:
        logging.warning(f"No monthly data files found containing ticker '{ticker}'.")
        return None
        
    df = pd.concat(dfs_to_load).drop_duplicates(subset=['date', 'ticker'])
    
    if df.empty:
        logging.warning(f"No data found for ticker '{ticker}' after filtering and concatenation.")
        return None
        
    logging.info(f"Found {len(df)} total records for '{ticker}'.")
    
    df['date'] = pd.to_datetime(df['date'])
    return df.set_index('date').sort_index()


# --- API Endpoints ---
@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({"status": "healthy", "service": "ta-api"}), 200

@app.route('/analyze', methods=['POST'])
def analyze_stock_data():
    """
    Receives a ticker, reads its data from the local Parquet store,
    and performs a full technical analysis.
    """
    req_data = request.get_json()
    ticker = req_data.get('ticker')

    if not ticker:
        return jsonify({"error": "Invalid request payload. Requires 'ticker'."}), 400

    df = get_data_from_local_store(ticker)

    if df is None or len(df) < 252:
        return jsonify({
            "ticker": ticker,
            "message": f"Not enough historical data for {ticker} to perform meaningful analysis."
        }), 200

    analysis_results = {"ticker": ticker, "patterns": [], "indicators": {}}

    try:
        analysis_results['indicators']['last_close'] = df['close'].iloc[-1]
        analysis_results['indicators']['RSI'] = round(ta.momentum.rsi(df['close'], window=14).iloc[-1], 2)
        macd_indicator = ta.trend.MACD(df['close'], window_fast=12, window_slow=26, window_sign=9)
        analysis_results['indicators']['MACD'] = round(macd_indicator.macd().iloc[-1], 2)
        bb_indicator = ta.volatility.BollingerBands(df['close'], window=20, window_dev=2)
        analysis_results['indicators']['BB_High'] = round(bb_indicator.bollinger_hband().iloc[-1], 2)
        analysis_results['indicators']['BB_Low'] = round(bb_indicator.bollinger_lband().iloc[-1], 2)
        log_returns = np.log(df['close'] / df['close'].shift(1))
        hv_30d = log_returns.rolling(window=30).std() * np.sqrt(252)
        analysis_results['indicators']['HV_30D_Annualized'] = round(hv_30d.iloc[-1] * 100, 2)
        
        return jsonify(analysis_results), 200

    except Exception as e:
        logging.error(f"Error in analyze_stock_data for {ticker}: {e}", exc_info=True)
        return jsonify({"error": str(e), "message": "Failed to perform technical analysis."}), 500


@app.route('/analyze-index/<index_symbol>', methods=['GET'])
def analyze_index(index_symbol):
    """Analyzes an index's current price relative to its 52-week range."""
    df = get_data_from_local_store(index_symbol)
    
    if df is None or len(df) < 252:
        return jsonify({
            "symbol": index_symbol,
            "error": f"Not enough data found for index {index_symbol}"
        }), 200

    try:
        high_52wk = df['high'].rolling(window=252).max().iloc[-1]
        low_52wk = df['low'].rolling(window=252).min().iloc[-1]
        last_close = df['close'].iloc[-1]
        
        rank = ((last_close - low_52wk) / (high_52wk - low_52wk)) * 100 if (high_52wk - low_52wk) != 0 else 50
        
        return jsonify({
            "symbol": index_symbol,
            "last_close": round(last_close, 2),
            "52_week_high": round(high_52wk, 2),
            "52_week_low": round(low_52wk, 2),
            "52_week_rank_percent": round(rank, 2)
        }), 200
    except Exception as e:
        logging.error(f"Error in analyze_index for {index_symbol}: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001, debug=False)
