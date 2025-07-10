import os
from datetime import date, timedelta
from flask import Flask, jsonify, request
import pandas as pd
import numpy as np 
import ta 

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

    df.columns = df.columns.str.lower()

    required_cols = ['open', 'high', 'low', 'close', 'volume']
    if not all(col in df.columns for col in required_cols):
        missing_cols = [col for col in required_cols if col not in df.columns]
        return jsonify({"error": f"Missing required OHLCV columns: {', '.join(missing_cols)}."}), 400

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
        # --- Price Indicators ---
        analysis_results['indicators']['RSI'] = round(ta.momentum.rsi(df['close'], window=14).iloc[-1], 2)
        macd_indicator = ta.trend.MACD(df['close'], window_fast=12, window_slow=26, window_sign=9)
        analysis_results['indicators']['MACD'] = round(macd_indicator.macd().iloc[-1], 2)
        analysis_results['indicators']['MACD_Signal'] = round(macd_indicator.macd_signal().iloc[-1], 2)
        analysis_results['indicators']['MACD_Hist'] = round(macd_indicator.macd_diff().iloc[-1], 2)
        analysis_results['indicators']['SMA_20'] = round(ta.trend.sma_indicator(df['close'], window=20).iloc[-1], 2)
        analysis_results['indicators']['SMA_50'] = round(ta.trend.sma_indicator(df['close'], window=50).iloc[-1], 2)

        # --- Volatility Indicators ---
        bb_indicator = ta.volatility.BollingerBands(df['close'], window=20, window_dev=2)
        analysis_results['indicators']['BB_High'] = round(bb_indicator.bollinger_hband().iloc[-1], 2)
        analysis_results['indicators']['BB_Low'] = round(bb_indicator.bollinger_lband().iloc[-1], 2)

        # --- Volume Indicators ---
        analysis_results['indicators']['OBV'] = round(ta.volume.on_balance_volume(df['close'], df['volume']).iloc[-1], 2)
        last_volume = df['volume'].iloc[-1]
        avg_volume_20 = ta.trend.sma_indicator(df['volume'], window=20).iloc[-1]
        analysis_results['indicators']['Is_High_Volume'] = bool(last_volume > avg_volume_20 * 1.5)

        # --- Pattern Recognition ---
        # SMA Crossover
        sma_20 = analysis_results['indicators']['SMA_20']
        sma_50 = analysis_results['indicators']['SMA_50']
        sma_20_prev = ta.trend.sma_indicator(df['close'], window=20).iloc[-2]
        sma_50_prev = ta.trend.sma_indicator(df['close'], window=50).iloc[-2]

        if sma_20 > sma_50 and sma_20_prev <= sma_50_prev:
            analysis_results['patterns'].append("SMA Crossover: 20-Day above 50-Day (Bullish)")
        elif sma_20 < sma_50 and sma_20_prev >= sma_50_prev:
            analysis_results['patterns'].append("SMA Crossover: 20-Day below 50-Day (Bearish)")
            
        # Bollinger Band Breakout/Breakdown
        last_close = df['close'].iloc[-1]
        if last_close > analysis_results['indicators']['BB_High']:
            analysis_results['patterns'].append("Price above Upper Bollinger Band (Overbought/Breakout)")
        elif last_close < analysis_results['indicators']['BB_Low']:
            analysis_results['patterns'].append("Price below Lower Bollinger Band (Oversold/Breakdown)")

        return jsonify(analysis_results), 200

    except Exception as e:
        app.logger.error(f"Error in analyze_stock_data for {ticker}: {e}", exc_info=True)
        return jsonify({"error": str(e), "message": "Failed to perform technical analysis."}), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001, debug=False)