# config.py

import os
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv()

# --- Load API Keys from Environment ---
# The os.getenv() function safely retrieves the variable.
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

# It's good practice to check if the keys were actually found
if not POLYGON_API_KEY:
    raise ValueError("POLYGON_API_KEY not found. Please set it in your .env file.")
if not GEMINI_API_KEY:
    raise ValueError("GEMINI_API_KEY not found. Please set it in your .env file.")


# --- Non-Sensitive Configurations ---
# These can still be defined directly in this file.
BUY_THRESHOLD = 0.4
SELL_THRESHOLD = -0.4
DATA_DIRECTORY = "data/"
ARTICLE_LIMIT = 10

# --- Master Stock Universe Setting ---
# Change this variable to switch between 'snp500' and 'qqq'
STOCK_UNIVERSE = "qqq"  # Options: "snp500", "qqq"