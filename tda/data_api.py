import os
from datetime import datetime, timedelta

import requests
from flask import Flask, jsonify, request
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

BASE_URL = "https://api.polygon.io"


def fetch_data_from_polygon(endpoint, params=None):
    """
    Helper function to fetch data from Polygon.io with error handling.

    Args:
        endpoint (str): The endpoint URL (without the base URL).
        params (dict, optional): Query parameters for the request. Defaults to None.

    Returns:
        tuple: (JSON response, status code).  Returns (None, status_code) on error.
    """
    url = f"{BASE_URL}{endpoint}"
    try:
        response = requests.get(url, params={'apiKey': POLYGON_API_KEY, **(params or {})})
        response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
        return response.json(), response.status_code
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from Polygon: {e}")
        # Log the error for debugging purposes in a production environment.
        # Consider using a logging library like `logging`.
        return None, getattr(e.response, 'status_code', 500)  # Return status code if available, else 500


@app.route('/most-active-stocks', methods=['GET'])
def get_most_active_stocks():
    """
    Fetches the top N most active stocks by volume, automatically going back
    in time until an active trading day with data is found.
    Default N is 100.
    """
    n = int(request.args.get('n', 100))  # Get 'n' from query parameters, default to 100

    today = datetime.now().date()
    attempt_date = today
    data = None
    status_code = 500  # Initialize with a default error status

    for i in range(7):  # Try up to 7 days back to find an active trading day
        date_str = attempt_date.strftime('%Y-%m-%d')
        endpoint = f"/v2/aggs/grouped/locale/US/market/STOCKS/{date_str}"
        response, status_code = fetch_data_from_polygon(endpoint)

        if response and response.get('results'):
            # Sort by volume and take the top N
            data = sorted(response['results'], key=lambda x: x['v'], reverse=True)[:n]
            status_code = 200
            break  # Exit the loop if data is found
        else:
            attempt_date -= timedelta(days=1)  # Go back one day

    if data:
        return jsonify({"results": data, "date": attempt_date.strftime('%Y-%m-%d')}), status_code
    else:
        return jsonify({"error": "Could not retrieve most active stocks after 7 attempts.", "date": today.strftime('%Y-%m-%d')}), status_code


@app.route('/historical-data/<ticker>', methods=['GET'])
def get_historical_data(ticker):
    """
    Fetches historical daily OHLCV data for a given ticker.
    Defaults to the last 90 days.
    """
    days = int(request.args.get('days', 90))  # Get 'days' from query parameters, default to 90
    to_date = datetime.now().date()
    from_date = to_date - timedelta(days=days)

    to_date_str = to_date.strftime('%Y-%m-%d')
    from_date_str = from_date.strftime('%Y-%m-%d')

    endpoint = f"/v2/aggs/ticker/{ticker}/range/1/day/{from_date_str}/{to_date_str}"
    response, status_code = fetch_data_from_polygon(endpoint)

    if response and response.get('results'):
        return jsonify(response), status_code
    else:
        error_message = response.get('error') if response else "Failed to retrieve historical data."
        return jsonify({"error": error_message, "ticker": ticker}), status_code


@app.route('/news/<ticker>', methods=['GET'])
def get_news_for_ticker(ticker):
    """
    Fetches recent news articles for a given ticker.
    Defaults to the last 7 days.
    """
    days = int(request.args.get('days', 7))  # Get 'days' from query parameters, default to 7
    to_date = datetime.now().date()
    from_date = to_date - timedelta(days=days)

    to_date_str = to_date.strftime('%Y-%m-%d')
    from_date_str = from_date.strftime('%Y-%m-%d')

    endpoint = "/v2/reference/news"
    params = {
        "ticker": ticker,
        "published_utc.gte": from_date_str,
        "published_utc.lte": to_date_str,
        "order": "published_utc",
        "sort": "desc"
    }

    response, status_code = fetch_data_from_polygon(endpoint, params)

    if response and response.get('results'):
        return jsonify(response), status_code
    else:
        error_message = response.get('error') if response else "Failed to retrieve news."
        return jsonify({"error": error_message, "ticker": ticker}), status_code


@app.route('/health', methods=['GET'])
def health_check():
    """Simple health check endpoint."""
    return jsonify({"status": "healthy", "service": "data-api"}), 200


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))