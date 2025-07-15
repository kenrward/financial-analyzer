# options_api.py
import pandas as pd
from flask import Flask, jsonify, request
import logging
import numpy as np

app = Flask(__name__)

# --- Setup Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({"status": "healthy", "service": "options-api"}), 200

@app.route('/analyze-volatility', methods=['POST'])
def analyze_volatility():
    """
    Analyzes an options chain to determine IV vs HV spread and volatility skew.
    Expects a JSON payload with: ticker, stock_price, options_chain, and historical_volatility.
    """
    payload = request.get_json()
    required_keys = ['ticker', 'stock_price', 'options_chain', 'historical_volatility']
    if not all(key in payload for key in required_keys):
        return jsonify({"error": "Invalid request payload. Missing required keys."}), 400

    try:
        ticker = payload['ticker']
        stock_price = float(payload['stock_price'])
        options_chain = payload['options_chain']
        hv_30d = float(payload['historical_volatility'])

        # 1. Load data and find the nearest expiration date (e.g., ~30 days out)
        df = pd.DataFrame(options_chain)
        df['expiration_date'] = pd.to_datetime(df['expiration_date'])
        df['dte'] = (df['expiration_date'] - pd.Timestamp.now()).dt.days
        
        # Target DTE around 30 days for standard analysis
        # Find the DTE that is >= 25 and closest to 30
        valid_dtes = df[df['dte'] >= 25]['dte']
        if valid_dtes.empty:
             return jsonify({"message": f"No options found with at least 25 DTE for {ticker}"}), 404
        
        nearest_30d_dte = valid_dtes.iloc[(valid_dtes - 30).abs().argsort()[:1]].iloc[0]
        df_30d = df[df['dte'] == nearest_30d_dte].copy()

        # 2. Find At-the-Money (ATM) Implied Volatility
        # Find the strike price closest to the current stock price
        atm_strike_row = df_30d.iloc[(df_30d['strike_price'] - stock_price).abs().argsort()[:1]]
        if atm_strike_row.empty:
            return jsonify({"message": f"Could not determine ATM strike for {ticker}"}), 404
            
        atm_iv = atm_strike_row['implied_volatility'].iloc[0] * 100 # As a percentage

        # 3. Calculate IV vs. HV Spread
        # This tells us if the implied volatility is "expensive" relative to actual stock movement
        iv_hv_spread = atm_iv - hv_30d

        # 4. Calculate Volatility Skew
        # We'll use the 25-delta put and call to measure skew
        calls_30d = df_30d[df_30d['contract_type'] == 'call']
        puts_30d = df_30d[df_30d['contract_type'] == 'put']
        
        # Ensure we have both calls and puts to analyze
        if calls_30d.empty or puts_30d.empty:
            return jsonify({"message": f"Could not find both calls and puts for DTE {nearest_30d_dte}"}), 404
        
        # Find the 25 delta call and put IVs
        iv_call_25d = calls_30d.iloc[(calls_30d['delta'] - 0.25).abs().argsort()[:1]]['implied_volatility'].iloc[0] * 100
        iv_put_25d = puts_30d.iloc[(puts_30d['delta'] - (-0.25)).abs().argsort()[:1]]['implied_volatility'].iloc[0] * 100
        
        # Skew is the difference. A positive value means puts are more expensive (fear).
        skew = iv_put_25d - iv_call_25d

        analysis_result = {
            "ticker": ticker,
            "atm_iv_percent": round(atm_iv, 2),
            "iv_hv_spread_percent": round(iv_hv_spread, 2),
            "skew_25_delta": round(skew, 2)
        }
        
        return jsonify(analysis_result), 200

    except Exception as e:
        logging.error(f"Error in analyze_volatility for {payload.get('ticker')}: {e}", exc_info=True)
        return jsonify({"error": str(e), "message": "Failed to perform volatility analysis."}), 500

if __name__ == '__main__':
    # Run with Gunicorn in production
    app.run(host='0.0.0.0', port=5002, debug=True)