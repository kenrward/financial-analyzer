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

@app.route('/health', methods=['GET'])
def health_check():
    """Simple health check endpoint."""
    return jsonify({"status": "healthy", "service": "data-api"}), 200

@app.route('/most-active-stocks', methods=['GET'])
def get_most_active_stocks():
    """Fetches the top N most active stocks for the previous trading day."""
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

@app.route('/news/<ticker>', methods=['GET'])
def get_news_for_ticker(ticker):
    """Fetches recent news articles for a given ticker from Polygon."""
    try:
        news_articles = list(client.list_ticker_news(ticker=ticker.upper(), limit=20))
        if not news_articles:
            return jsonify({"ticker": ticker.upper(), "news": []}), 200

        formatted_news = [
            {"title": article.title, "publisher": article.publisher.name, "published_utc": article.published_utc, "article_url": article.article_url}
            for article in news_articles
        ]
        return jsonify({"ticker": ticker.upper(), "news": formatted_news}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/earnings-calendar/<ticker>', methods=['GET'])
def get_earnings_calendar(ticker):
    """Fetches upcoming earnings dates for a given ticker from Yahoo Finance."""
    try:
        stock = yf.Ticker(ticker)
        earnings_dates = stock.earnings_dates
        if earnings_dates is None or earnings_dates.empty:
            return jsonify({"ticker": ticker.upper(), "earnings": []}), 200
        
        formatted_earnings = [{"report_date": index.strftime('%Y-%m-%d')} for index, row in earnings_dates.iterrows()]
        return jsonify({"ticker": ticker.upper(), "earnings": formatted_earnings[-8:]}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/dividends/<ticker>', methods=['GET'])
def get_dividends(ticker):
    """Fetches ex-dividend dates for a given ticker from Yahoo Finance."""
    try:
        stock = yf.Ticker(ticker)
        dividends = stock.dividends
        if dividends is None or dividends.empty:
            return jsonify({"ticker": ticker.upper(), "dividends": []}), 200

        formatted_dividends = [{"ex_dividend_date": index.strftime('%Y-%m-%d')} for index, value in dividends.items()]
        return jsonify({"ticker": ticker.upper(), "dividends": formatted_dividends[-8:]}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/options-chain/<ticker>', methods=['GET'])
def get_options_chain(ticker):
    """Fetches the full options chain snapshot for a given ticker from Polygon."""
    try:
        chain_iterator = client.list_snapshot_options_chain(ticker)
        formatted_chain = []
        for contract in chain_iterator:
            if not hasattr(contract, 'greeks') or contract.greeks is None or not hasattr(contract, 'implied_volatility'):
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
            return jsonify({"ticker": ticker.upper(), "options_chain": []}), 200
            
        return jsonify({"ticker": ticker.upper(), "options_chain": formatted_chain}), 200
    except Exception as e:
        app.logger.error(f"Error in get_options_chain for {ticker}: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)