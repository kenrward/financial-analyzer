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
    Fetches the top N most active stocks by volume, automatically going back
    in time until an active trading day with data is found.
    Default N is 100.
    """
    top_n = request.args.get('limit', default=100, type=int)

    # Start looking from the very first day we might need to check.
    # Today's market data is not available until end of day, so start from yesterday.
    current_date_to_check = date.today() - timedelta(days=1)

    found_active_day = False
    lookback_attempts = 0
    max_calendar_days_to_check = 15 # Look back up to 15 calendar days to find an active trading day

    active_stocks = []
    final_date_str = None

    # Loop until an active trading day with data is found or max lookback reached
    while not found_active_day and lookback_attempts < max_calendar_days_to_check:
        # Skip weekends (Saturday=5, Sunday=6) before trying to fetch data for this date
        while current_date_to_check.weekday() >= 5:
            current_date_to_check -= timedelta(days=1)

        target_date_str = current_date_to_check.strftime('%Y-%m-%d')
        app.logger.info(f"Attempting to fetch data for {target_date_str} (Attempt: {lookback_attempts + 1}/{max_calendar_days_to_check})")

        try:
            resp = client.get_grouped_daily_aggs(
                date=target_date_str,
                adjusted=True
            )

            if resp: # If resp is not an empty list, it means data was found
                # Filter out OTC and sort by volume
                temp_active_stocks = sorted(
                    [s for s in resp if not getattr(s, 'otc', False)],
                    key=lambda x: x.v,
                    reverse=True
                )[:top_n]

                if temp_active_stocks: # Ensure there are actual stocks after filtering (e.g., not just OTC)
                    active_stocks = temp_active_stocks
                    final_date_str = target_date_str
                    found_active_day = True
                    app.logger.info(f"Successfully found active stocks for {final_date_str}.")
                else:
                    app.logger.info(f"No non-OTC active stocks found for {target_date_str}. Decrementing date.")
            else: # resp was an empty list (no data for that trading day, e.g., a holiday)
                app.logger.info(f"Polygon.io returned no data for {target_date_str}. Decrementing date.")

        except Exception as e:
            # Log API errors but continue trying previous days
            app.logger.warning(f"API error fetching data for {target_date_str}: {e}. Decrementing date.")

        # For the next iteration, move to the previous calendar day
        if not found_active_day: # Only decrement if we haven't found data yet
            current_date_to_check -= timedelta(days=1)

        lookback_attempts += 1 # Increment attempt counter


    if not active_stocks:
        return jsonify({"message": f"No active stocks found after looking back {max_calendar_days_to_check} calendar days.", "stocks": []}), 404

    # Format the output
    formatted_stocks = [
        {
            "ticker": stock.T,
            "volume": stock.v,
            "close_price": stock.c,
            "open_price": stock.o,
            "high_price": stock.h,
            "low_price": stock.l,
            "date": final_date_str # Use the date of the day we found data for
        }
        for stock in active_stocks
    ]
    return jsonify({"date": final_date_str, "top_stocks": formatted_stocks}), 200

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
        for article in client.list_reference_news(
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
    app.run(host='0.0.0.0', port=5000, debug=False) # Set debug=False for production