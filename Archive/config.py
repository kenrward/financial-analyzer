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
ARTICLE_LIMIT = 25
REQUIRE_RECENT_NEWS = False # Set to False to ignore the news check

# --- Screener Configuration ---
# Set to True to run the screener, False to analyze the full index
ENABLE_SCREENER = True
MIN_OPTIONS_VOLUME = 50 #20000
MIN_IMPLIED_VOLATILITY = 0.0

# --- Master Stock Universe Setting ---
# Change this variable to switch between 'snp500' and 'qqq'
STOCK_UNIVERSE = "snp500"  # Options: "snp500", "qqq"

# --- NEW: Local DeepSeek Configuration ---
# The base URL for your local LLM server.
# You may need to adjust the port (e.g., 8000, 5000) depending on your server setup.
DEEPSEEK_API_BASE_URL = "http://192.168.86.6:11434/api/generate"
# The model name as defined by your local server.
DEEPSEEK_MODEL_NAME = "llama3" # Example model name, change if needed
# The API key for local servers is often not required or can be any string.
DEEPSEEK_API_KEY = "EMPTY"

# The host URL for your Ollama server from your curl command
OLLAMA_HOST = "http://192.168.86.36:11434"
# The model you want to use that is served by Ollama
# OLLAMA_MODEL_NAME = "llama3"
# 
# 
# 
# OLLAMA_MODEL_NAME = "llama3:8b-instruct-q4_K_M"
OLLAMA_MODEL_NAME = "deepseek-llm"
