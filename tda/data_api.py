# data_api.py

import os
from datetime import date, timedelta
from flask import Flask, jsonify, request
from polygon import RESTClient
from dotenv import load_dotenv
import yfinance as yf

load_dotenv()

app = Flask(__name__)

POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
if not POLYGON_API_KEY:
    raise ValueError("POLYGON_API_KEY environment variable not set.")

client = RESTClient(api_key=POLYGON_API_KEY)


# --- V1 & Existing Endpoints (Unchanged) ---
@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({"status": "healthy", "service": "data-api"}), 200

@app.route('/most-active-stocks', methods=['GET'])
def get_most_active_stocks():
    top_n = request.args.get('limit', default=100, type=int)
    target_day = date.today() - timedelta(days=1)
    for _ in range(15):
        try:
            target_date_str = target_day.strftime('%Y-%m-%d')
            resp = client.get_grouped_daily_aggs(date=target_date_str, adjusted=True)
            if resp:
                active_stocks = sorted(resp, key=lambda x: x.volume, reverse=True)[:top_n]
                formatted_stocks = [
                    {"ticker": stock.ticker, "volume": stock.volume, "close_price": stock.close}
                    for stock in active_stocks if not getattr(stock, 'otc', False)
                ]
                return jsonify({"date": target_date_str, "top_stocks": formatted_stocks}), 200
        except Exception:
            pass 
        target_day -= timedelta(days=1)
    return jsonify({"message": "Could not find recent trading data."}), 404

# ... (other existing endpoints like /historical-data, /news, /earnings-calendar, /dividends are also unchanged) ...
@app.route('/historical-data/<ticker>', methods=['GET'])
def get_historical_data(ticker):
    days = request.args.get('days', default=365, type=int)
    to_date = date.today()
    from_date = to_date - timedelta(days=days)
    try:
        aggs = list(client.list_aggs(
            ticker=ticker.upper(), multiplier=1, timespan="day", from_=from_date.strftime('%Y-%m-%d'),
            to=to_date.strftime('%Y-%m-%d'), adjusted=True, limit=50000
        ))
        if not aggs: return jsonify({"message": f"No historical data for {ticker}"}), 404
        formatted_aggs = [
            {"date": date.fromtimestamp(a.timestamp / 1000).strftime('%Y-%m-%d'), "open": a.open, "high": a.high, "low": a.low, "close": a.close, "volume": a.volume}
            for a in aggs
        ]
        return jsonify({"ticker": ticker.upper(), "data": formatted_aggs}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/news/<ticker>', methods=['GET'])
def get_news_for_ticker(ticker):
    try:
        news_articles = list(client.list_ticker_news(ticker=ticker.upper(), limit=20))
        if not news_articles: return jsonify({"message": f"No recent news for {ticker}"}), 404
        formatted_news = [
            {"title": article.title, "publisher": article.publisher.name, "published_utc": article.published_utc, "article_url": article.article_url}
            for article in news_articles
        ]
        return jsonify({"ticker": ticker.upper(), "news": formatted_news}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/earnings-calendar/<ticker>', methods=['GET'])
def get_earnings_calendar(ticker):
    try:
        stock = yf.Ticker(ticker)
        earnings_dates = stock.earnings_dates
        if earnings_dates is None or earnings_dates.empty:
            return jsonify({"message": f"No earnings data for {ticker}"}), 404
        formatted_earnings = [{"report_date": index.strftime('%Y-%m-%d')} for index, row in earnings_dates.iterrows()]
        return jsonify({"ticker": ticker.upper(), "earnings": formatted_earnings[-8:]}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/dividends/<ticker>', methods=['GET'])
def get_dividends(ticker):
    try:
        stock = yf.Ticker(ticker)
        dividends = stock.dividends
        if dividends is None or dividends.empty:
            return jsonify({"message": f"No dividend data for {ticker}"}), 404
        formatted_dividends = [{"ex_dividend_date": index.strftime('%Y-%m-%d')} for index, value in dividends.items()]
        return jsonify({"ticker": ticker.upper(), "dividends": formatted_dividends[-8:]}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# --- ✅ V2: Options Chain Endpoint (Corrected) ---
@app.route('/options-chain/<ticker>', methods=['GET'])
def get_options_chain(ticker):
    """Fetches the full options chain snapshot for a given ticker from Polygon."""
    try:
        # CORRECTED: Using the proper function name from the docs
        chain_iterator = client.list_snapshot_options_chain(ticker)
        
        formatted_chain = []
        for contract in chain_iterator:
            # Skip contracts with no greeks data, as they can't be analyzed
            if not hasattr(contract, 'greeks') or contract.greeks is None:
                continue
            
            formatted_chain.append({
                "ticker": contract.details.ticker,
                "expiration_date": contract.details.expiration_date,
                "strike_price": contract.details.strike_price,
                "contract_type": contract.details.contract_type,
                "implied_volatility": contract.implied_volatility,
                "delta": contract.greeks.delta
            })
        
        if not formatted_chain:
            return jsonify({"message": f"No options chain data with greeks found for {ticker}"}), 404

        return jsonify({"ticker": ticker.upper(), "options_chain": formatted_chain}), 200
    except Exception as e:
        app.logger.error(f"Error in get_options_chain for {ticker}: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)