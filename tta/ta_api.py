# ta_api.py
import os
import pandas as pd
import numpy as np 
import ta 
from flask import Flask, jsonify, request
import logging

app = Flask(__name__)

# --- Configuration ---
DATA_PATH = "/mnt/shared-drive/us_stocks_daily.parquet"
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Helper function with efficient data filtering ---
def get_data_from_local_store(ticker: str):
    """
    Reads only the necessary rows for a specific ticker from the Parquet store
    using predicate pushdown filtering.
    """
    try:
        logging.info(f"Reading data for ticker '{ticker}' from: {DATA_PATH}")
        
        # Use the 'filters' argument to read only the rows where the 'ticker' column matches.
        ticker_df = pd.read_parquet(DATA_PATH, filters=[('ticker', '==', ticker)])

        if ticker_df.empty:
            logging.warning(f"No data found for ticker '{ticker}' after filtering.")
            return None
            
        logging.info(f"Found {len(ticker_df)} records for '{ticker}'.")
        
        # Process the filtered dataframe
        ticker_df['date'] = pd.to_datetime(ticker_df['date'])
        return ticker_df.set_index('date').sort_index()

    except FileNotFoundError:
        logging.error(f"FATAL: Master data file not found at {DATA_PATH}")
        return None
    except Exception as e:
        logging.error(f"Failed to read or process local data file for {ticker}: {e}")
        return None

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

    if df is None or len(df) < 252: # Require at least a year of data
        return jsonify({"message": f"Not enough historical data for {ticker} to perform meaningful analysis."}), 404

    analysis_results = {"ticker": ticker, "patterns": [], "indicators": {}}

    try:
        # --- Add the last closing price to the results ---
        analysis_results['indicators']['last_close'] = df['close'].iloc[-1]
        
        # --- Standard Indicators ---
        analysis_results['indicators']['RSI'] = round(ta.momentum.rsi(df['close'], window=14).iloc[-1], 2)
        macd_indicator = ta.trend.MACD(df['close'], window_fast=12, window_slow=26, window_sign=9)
        analysis_results['indicators']['MACD'] = round(macd_indicator.macd().iloc[-1], 2)
        
        # --- Volatility Indicators ---
        bb_indicator = ta.volatility.BollingerBands(df['close'], window=20, window_dev=2)
        analysis_results['indicators']['BB_High'] = round(bb_indicator.bollinger_hband().iloc[-1], 2)
        analysis_results['indicators']['BB_Low'] = round(bb_indicator.bollinger_lband().iloc[-1], 2)

        # --- Historical Volatility (HV) ---
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
        return jsonify({"error": f"Not enough data found for index {index_symbol}"}), 404

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