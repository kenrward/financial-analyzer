# data_api.py

import os
from datetime import date, timedelta
from flask import Flask, jsonify, request
from polygon import RESTClient
from dotenv import load_dotenv
import requests # Import the requests library

load_dotenv()

app = Flask(__name__)

POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
if not POLYGON_API_KEY:
    raise ValueError("POLYGON_API_KEY environment variable not set.")

# This client is still used for the V1 endpoints
client = RESTClient(api_key=POLYGON_API_KEY)


# --- V1 Endpoints (Unchanged) ---
@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({"status": "healthy", "service": "data-api"}), 200

# ... (other V1 endpoints like /most-active-stocks, /historical-data, /news are unchanged) ...
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

@app.route('/historical-data/<ticker>', methods=['GET'])
def get_historical_data(ticker):
    days = request.args.get('days', default=90, type=int)
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


# --- ✅ V2 Endpoints (Rewritten with the 'requests' library) ---

def _get_corporate_events(ticker, event_type):
    """Helper function to call the TMX corporate events endpoint using requests."""
    base_url = "https://api.polygon.io/tmx/v1/corporate-events"
    params = {
        "ticker": ticker.upper(),
        "type": event_type,
        "apiKey": POLYGON_API_KEY
    }
    response = requests.get(base_url, params=params)
    response.raise_for_status() # Raise an exception for bad status codes (4xx or 5xx)
    return response.json().get('results', [])

@app.route('/earnings-calendar/<ticker>', methods=['GET'])
def get_earnings_calendar(ticker):
    """Fetches earnings announcement dates for a given ticker."""
    try:
        earnings = _get_corporate_events(ticker, "earnings_announcement_date")
        if not earnings:
            return jsonify({"message": f"No earnings data for {ticker}"}), 404
        
        formatted_earnings = [{"report_date": e.get('date')} for e in earnings]
        return jsonify({"ticker": ticker.upper(), "earnings": formatted_earnings}), 200
    except Exception as e:
        app.logger.error(f"Error in get_earnings_calendar for {ticker}: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500

@app.route('/dividends/<ticker>', methods=['GET'])
def get_dividends(ticker):
    """Fetches ex-dividend dates for a given ticker."""
    try:
        dividends = _get_corporate_events(ticker, "dividend")
        if not dividends:
            return jsonify({"message": f"No dividend data for {ticker}"}), 404

        formatted_dividends = [{"ex_dividend_date": d.get('date')} for d in dividends]
        return jsonify({"ticker": ticker.upper(), "dividends": formatted_dividends}), 200
    except Exception as e:
        app.logger.error(f"Error in get_dividends for {ticker}: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)