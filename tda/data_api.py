import os
from datetime import date, timedelta
from flask import Flask, jsonify, request
from polygon import RESTClient
from dotenv import load_dotenv

# Load environment variables from .env file
# Ensure this script is run from a directory where .env exists,
# or specify the path to the .env file if it's elsewhere.
load_dotenv()

app = Flask(__name__)

# Retrieve Polygon API Key from environment variable
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
if not POLYGON_API_KEY:
    raise ValueError("POLYGON_API_KEY environment variable not set.")

# Initialize Polygon REST Client
client = RESTClient(api_key=POLYGON_API_KEY)

@app.route('/health', methods=['GET'])
def health_check():
    """Simple health check endpoint."""
    return jsonify({"status": "healthy", "service": "data-api"}), 200

@app.route('/most-active-stocks', methods=['GET'])
def get_most_active_stocks():
    """
    Fetches the top N most active stocks by volume for the previous trading day.
    Default N is 100.
    """
    top_n = request.args.get('limit', default=100, type=int)

    # Calculate the previous trading day's date
    today = date.today()
    # Start from yesterday and go backwards until we find a weekday
    prev_trading_day = today - timedelta(days=1)
    while prev_trading_day.weekday() >= 5: # Monday is 0, Sunday is 6 (for weekend)
        prev_trading_day -= timedelta(days=1)

    target_date_str = prev_trading_day.strftime('%Y-%m-%d')

    try:
        # get_grouped_daily_aggs returns a list of Aggregate objects directly
        # The 'market' and 'locale' are implicit for this specific function
        resp = client.get_grouped_daily_aggs(
            date=target_date_str,
            adjusted=True  # Always use adjusted data for consistency
        )

        if not resp: # Check if the list of aggregates is empty
            return jsonify({"message": f"No data found for {target_date_str}", "stocks": []}), 404

        # Sort by volume and filter out OTC tickers
        active_stocks = sorted(
            [s for s in resp if not getattr(s, 'otc', False)], # Use getattr for 'otc' as it might be missing if false/not present
            key=lambda x: x.volume, # Corrected attribute access: from x.v to x.volume
            reverse=True
        )[:top_n]

        # Format the output to be concise
        formatted_stocks = [
            {
                "ticker": stock.ticker,        # Corrected attribute access: from stock.T to stock.ticker
                "volume": stock.volume,        # Corrected attribute access: from stock.v to stock.volume
                "close_price": stock.close,    # Corrected attribute access: from stock.c to stock.close
                "open_price": stock.open,      # Corrected attribute access: from stock.o to stock.open
                "high_price": stock.high,      # Corrected attribute access: from stock.h to stock.high
                "low_price": stock.low,        # Corrected attribute access: from stock.l to stock.low
                "date": target_date_str
            }
            for stock in active_stocks
        ]
        return jsonify({"date": target_date_str, "top_stocks": formatted_stocks}), 200

    except Exception as e:
        # Log the full error for debugging on the server side
        app.logger.error(f"Error in get_most_active_stocks: {e}", exc_info=True)
        return jsonify({"error": str(e), "message": "Failed to retrieve most active stocks."}), 500

@app.route('/historical-data/<ticker>', methods=['GET'])
def get_historical_data(ticker):
    """
    Fetches historical daily OHLCV data for a given ticker.
    Defaults to the last 90 days.
    """
    days = request.args.get('days', default=90, type=int)

    to_date = date.today()
    from_date = to_date - timedelta(days=days)

    try:
        aggs = []
        # list_aggs returns an iterator, so we collect all results
        for a in client.list_aggs(
            ticker=ticker.upper(),
            multiplier=1,
            timespan="day",
            from_=from_date.strftime('%Y-%m-%d'),
            to=to_date.strftime('%Y-%m-%d'),
            adjusted=True,
            sort="asc",
            limit=50000 # Max limit to ensure we get all data in range
        ):
            aggs.append({
                "date": date.fromtimestamp(a.timestamp / 1000).strftime('%Y-%m-%d'), # Corrected: a.t to a.timestamp
                "open": a.open,       # Corrected: a.o to a.open
                "high": a.high,       # Corrected: a.h to a.high
                "low": a.low,         # Corrected: a.l to a.low
                "close": a.close,     # Corrected: a.c to a.close
                "volume": a.volume    # Corrected: a.v to a.volume
            })

        if not aggs:
            return jsonify({"message": f"No historical data found for {ticker} for the last {days} days."}), 404

        return jsonify({"ticker": ticker.upper(), "data": aggs}), 200

    except Exception as e:
        app.logger.error(f"Error in get_historical_data for {ticker}: {e}", exc_info=True)
        return jsonify({"error": str(e), "message": f"Failed to retrieve historical data for {ticker}."}), 500

@app.route('/news/<ticker>', methods=['GET'])
def get_news_for_ticker(ticker):
    """
    Fetches recent news articles for a given ticker.
    Defaults to the last 7 days.
    """
    days = request.args.get('days', default=7, type=int)

    to_date = date.today()
    from_date = to_date - timedelta(days=days)

    try:
        news_articles = []
        # list_reference_news returns an iterator
        for article in client.list_news(
            ticker=ticker.upper(),
            published_utc_gte=from_date.strftime('%Y-%m-%d'),
            published_utc_lte=to_date.strftime('%Y-%m-%d'),
            limit=50 # Max 50 articles for a concise response
        ):
            # These attributes appear to be stable based on docs and common usage
            news_articles.append({
                "title": article.title,
                "publisher": article.publisher.name if article.publisher else "N/A",
                "url": article.article_url,
                "published_utc": article.published_utc,
                "description": article.description
            })

        if not news_articles:
            return jsonify({"message": f"No recent news found for {ticker} in the last {days} days."}), 404

        return jsonify({"ticker": ticker.upper(), "news": news_articles}), 200

    except Exception as e:
        app.logger.error(f"Error in get_news_for_ticker for {ticker}: {e}", exc_info=True)
        return jsonify({"error": str(e), "message": f"Failed to retrieve news for {ticker}."}), 500


if __name__ == '__main__':
    # When running with `python3 data_api.py`, Flask's default development server is used.
    # For production, use a WSGI server like Gunicorn or uWSGI (e.g., install gunicorn: pip install gunicorn)
    # Then run with: gunicorn --bind 0.0.0.0:5000 data_api:app
    app.run(host='0.0.0.0', port=5000, debug=True) # Set debug=False for production