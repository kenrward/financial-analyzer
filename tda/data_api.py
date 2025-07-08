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





@app.route('/health', methods=['GET'])
def health_check():
    """Simple health check endpoint."""
    return jsonify({"status": "healthy", "service": "data-api"}), 200


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))