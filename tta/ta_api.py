import os
from datetime import date, timedelta
from flask import Flask, jsonify, request
import pandas as pd
import numpy as np # Explicitly import numpy for NaN
import ta # Changed from pandas_ta as ta

app = Flask(__name__)

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({"status": "healthy", "service": "ta-api"}), 200

@app.route('/analyze', methods=['POST'])
def analyze_stock_data():
    """
    Receives historical stock data (OHLCV) and performs technical analysis.
    Expects JSON with 'ticker' and 'data' (list of OHLCV dictionaries).
    """
    req_data = request.get_json()

    if not req_data or 'ticker' not in req_data or 'data' not in req_data:
        return jsonify({"error": "Invalid request payload. Requires 'ticker' and 'data'."}), 400

    ticker = req_data['ticker']
    ohlcv_data = req_data['data']

    if not ohlcv_data:
        return jsonify({"message": f"No data provided for {ticker} to analyze.", "analysis": {}}), 200

    df = pd.DataFrame(ohlcv_data)
    df['date'] = pd.to_datetime(df['date'])
    df = df.set_index('date')

    # The 'ta' library typically expects 'open', 'high', 'low', 'close', 'volume' (lowercase)
    # The data_api sends lowercase column names, so this rename might not be strictly necessary
    # if the input is already correct, but it ensures consistency.
    # It's safer to ensure they are consistent.
    # We'll rename if the input format uses 'open_price' etc., otherwise ensure they are lowercase.
    df.columns = df.columns.str.lower() # Convert all column names to lowercase to match 'ta' expectations

    # Ensure the required columns exist after potential renaming/conversion
    required_cols = ['open', 'high', 'low', 'close', 'volume']
    if not all(col in df.columns for col in required_cols):
        missing_cols = [col for col in required_cols if col not in df.columns]
        return jsonify({"error": f"Missing required OHLCV columns: {', '.join(missing_cols)}. Ensure input data matches expected format (lowercase open, high, low, close, volume).", "ticker": ticker}), 400


    if len(df) < 50: # Ensure enough data for common indicators
        last_close = df['close'].iloc[-1] if not df.empty else None
        return jsonify({
            "message": f"Not enough data for {ticker} to perform meaningful analysis. Need at least 50 periods. Last Close: {last_close}",
            "ticker": ticker,
            "patterns": [],
            "indicators": {}
        }), 200

    analysis_results = {
        "ticker": ticker,
        "patterns": [],
        "indicators": {}
    }

    try:
        # --- Calculate Indicators using 'ta' library ---
        # Relative Strength Index (RSI)
        rsi_series = ta.momentum.rsi(df['close'], window=14)
        if not rsi_series.empty and pd.notna(rsi_series.iloc[-1]):
            analysis_results['indicators']['RSI'] = round(rsi_series.iloc[-1], 2)

        # Moving Average Convergence Divergence (MACD)
        macd_data = ta.trend.macd(df['close'], window_fast=12, window_slow=26, window_sign=9)
        if not macd_data.empty and pd.notna(macd_data['MACD_12_26_9'].iloc[-1]):
            analysis_results['indicators']['MACD'] = round(macd_data['MACD_12_26_9'].iloc[-1], 2)
            analysis_results['indicators']['MACD_Signal'] = round(macd_data['MACDS_12_26_9'].iloc[-1], 2)
            analysis_results['indicators']['MACD_Hist'] = round(macd_data['MACDH_12_26_9'].iloc[-1], 2)

        # Simple Moving Averages (SMA)
        sma_20_series = ta.trend.sma_indicator(df['close'], window=20)
        if not sma_20_series.empty and pd.notna(sma_20_series.iloc[-1]):
            analysis_results['indicators']['SMA_20'] = round(sma_20_series.iloc[-1], 2)

        sma_50_series = ta.trend.sma_indicator(df['close'], window=50)
        if not sma_50_series.empty and pd.notna(sma_50_series.iloc[-1]):
            analysis_results['indicators']['SMA_50'] = round(sma_50_series.iloc[-1], 2)

        # --- Candlestick Pattern Recognition (requires manual implementation or a dedicated library) ---
        # The 'ta' library does not have built-in candlestick pattern recognition like pandas_ta.
        # We'll keep the SMA crossovers as an example "pattern" for now.

        # Simple SMA crossover signals
        if 'SMA_20' in analysis_results['indicators'] and 'SMA_50' in analysis_results['indicators'] and len(df) >= 2:
            sma_20_current = analysis_results['indicators']['SMA_20']
            sma_50_current = analysis_results['indicators']['SMA_50']

            # Need previous day's SMA values for a true crossover detection
            # Ensure these series are also not empty before accessing previous elements
            sma_20_prev_series = ta.trend.sma_indicator(df['close'], window=20)
            sma_50_prev_series = ta.trend.sma_indicator(df['close'], window=50)

            if not sma_20_prev_series.empty and not sma_50_prev_series.empty and len(sma_20_prev_series) >= 2 and len(sma_50_prev_series) >= 2:
                sma_20_prev = sma_20_prev_series.iloc[-2]
                sma_50_prev = sma_50_prev_series.iloc[-2]

                if sma_20_current > sma_50_current and sma_20_prev <= sma_50_prev:
                    analysis_results['patterns'].append("SMA Crossover: 20-Day above 50-Day (Bullish)")
                elif sma_20_current < sma_50_current and sma_20_prev >= sma_50_prev:
                    analysis_results['patterns'].append("SMA Crossover: 20-Day below 50-Day (Bearish)")

        return jsonify(analysis_results), 200

    except Exception as e:
        app.logger.error(f"Error in analyze_stock_data: {e}", exc_info=True)
        return jsonify({"error": str(e), "message": "Failed to perform technical analysis."}), 500


if __name__ == '__main__':
    # Listen on all available network interfaces
    app.run(host='0.0.0.0', port=5001, debug=False)