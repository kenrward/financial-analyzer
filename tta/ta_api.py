# ta_api.py
import os
import pandas as pd
import numpy as np 
import ta 
from flask import Flask, jsonify, request
import logging

app = Flask(__name__)
DATA_PATH = "/mnt/shared-drive/us_stocks_daily.parquet"
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_data_from_local_store(ticker: str):
    # ... (function is unchanged) ...

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({"status": "healthy", "service": "ta-api"}), 200

@app.route('/analyze', methods=['POST'])
def analyze_stock_data():
    req_data = request.get_json()
    ticker = req_data.get('ticker')
    if not ticker:
        return jsonify({"error": "Invalid request payload. Requires 'ticker'."}), 400

    df = get_data_from_local_store(ticker)
    
    # ✅ THE FIX: Return a specific message with a 200 OK status
    if df is None or len(df) < 252:
        return jsonify({
            "ticker": ticker,
            "message": f"Not enough historical data for {ticker} to perform meaningful analysis."
        }), 200

    analysis_results = {"ticker": ticker, "patterns": [], "indicators": {}}
    try:
        # ... (rest of analysis logic is unchanged) ...
        return jsonify(analysis_results), 200
    except Exception as e:
        logging.error(f"Error in analyze_stock_data for {ticker}: {e}", exc_info=True)
        return jsonify({"error": str(e), "message": "Failed to perform technical analysis."}), 500

@app.route('/analyze-index/<index_symbol>', methods=['GET'])
def analyze_index(index_symbol):
    df = get_data_from_local_store(index_symbol)
    
    # ✅ THE FIX: Return a specific message with a 200 OK status
    if df is None or len(df) < 252:
        return jsonify({
            "symbol": index_symbol,
            "error": f"Not enough data found for index {index_symbol}"
        }), 200

    try:
        # ... (rest of analysis logic is unchanged) ...
        return jsonify({
            "symbol": index_symbol, "last_close": round(last_close, 2),
            "52_week_high": round(high_52wk, 2), "52_week_low": round(low_52wk, 2),
            "52_week_rank_percent": round(rank, 2)
        }), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001, debug=False)
