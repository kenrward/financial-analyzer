import httpx
from langchain.tools import StructuredTool
import json
from pydantic import BaseModel, Field
import asyncio

# --- Configuration ---
DATA_API_BASE_URL = "https://tda.kewar.org"
TA_API_BASE_URL = "https://tta.kewar.org"
http_client = httpx.Client(verify=False)

# --- Helper to make HTTP calls and handle basic errors ---
def _make_api_call(url: str, method: str = "GET", params: dict = None, json_data: dict = None):
    """Helper to make HTTP calls and handle basic errors."""
    try:
        if method == "GET":
            response = http_client.get(url, params=params, timeout=30)
        elif method == "POST":
            response = http_client.post(url, json=json_data, timeout=60)
        else:
            raise ValueError(f"Unsupported HTTP method: {method}")

        response.raise_for_status() # Raises HTTPStatusError for bad responses (4xx or 5xx)
        return response.json()
    except httpx.HTTPStatusError as e:
        print(f"HTTP error occurred: {e.response.status_code} - {e.response.text}")
        return {"error": f"API call failed with status {e.response.status_code}: {e.response.text}"}
    except httpx.RequestError as e:
        print(f"Network or request error occurred: {e}")
        return {"error": f"API call failed: {e}"}
    except json.JSONDecodeError:
        print(f"JSON decode error: {response.text}")
        return {"error": f"Invalid JSON response from API: {response.text}"}
    except Exception as e:
        print(f"An unexpected error occurred during API call: {e}")
        return {"error": f"An unexpected error occurred: {e}"}

# --- Internal Component Functions ---
def _get_most_active_stocks(limit: int = 100) -> str:
    url = f"{DATA_API_BASE_URL}/most-active-stocks"
    params = {"limit": limit}
    response = _make_api_call(url, params=params)
    return json.dumps(response)

def _get_historical_data(ticker: str, days: int = 90) -> str:
    url = f"{DATA_API_BASE_URL}/historical-data/{ticker}"
    params = {"days": days}
    response = _make_api_call(url, params=params)
    return json.dumps(response)

# --- Add this new helper function ---
def _get_ticker_details(ticker: str) -> dict:
    """Gets detailed information for a single ticker, including optionability."""
    url = f"https://api.polygon.io/v3/reference/tickers/{ticker}"
    # The POLYGON_API_KEY must be available as an environment variable
    # This call is slightly different and needs the key in the params
    params = {"apiKey": os.getenv("POLYGON_API_KEY")} 
    response = _make_api_call(url, params=params)
    return response

def _get_news_for_ticker(ticker: str, days: int = 7) -> str:
    url = f"{DATA_API_BASE_URL}/news/{ticker}"
    params = {"days": days}
    response = _make_api_call(url, params=params)
    return json.dumps(response)

def _analyze_technical_patterns(ticker: str, historical_data_json: str) -> str:
    url = f"{TA_API_BASE_URL}/analyze"
    try:
        data_payload = json.loads(historical_data_json)
    except json.JSONDecodeError:
        return json.dumps({"error": "Invalid historical_data_json format provided to analyze_technical_patterns."})
    response = _make_api_call(url, method="POST", json_data=data_payload)
    return json.dumps(response)

def _get_and_analyze_ticker(ticker: str, days: int = 90) -> str:
    """Gets historical data and immediately runs technical analysis on it."""
    print(f"--- Running combined analysis for {ticker} ---")
    
    # Enforce a minimum lookback period to ensure TA indicators can be calculated.
    if days < 50:
        print(f"--- Requested days ({days}) is less than 50. Overriding to 90. ---")
        days = 90
        
    # Step 1: Get historical data
    historical_data_str = _get_historical_data(ticker, days)
    historical_data_json = json.loads(historical_data_str)
    
    if "error" in historical_data_json:
        return json.dumps(historical_data_json)
        
    # Step 2: Run technical analysis on the data we just got
    analysis_result_str = _analyze_technical_patterns(ticker, historical_data_str)
    return analysis_result_str

# --- The "Super-Tool" Function ---
async def _find_and_analyze_active_stocks(limit: int = 5) -> str:
    """
    Master tool that finds active stocks, filters for optionable ones, 
    and then runs the full analysis.
    """
    print(f"--- 🚀 Kicking off full analysis for top {limit} stocks ---")
    
    # Step 1: Get the most active stocks
    active_stocks_str = _get_most_active_stocks(limit)
    active_stocks_data = json.loads(active_stocks_str)

    if "error" in active_stocks_data or not active_stocks_data.get("top_stocks"):
        return json.dumps({"error": "Could not retrieve the list of active stocks."})

    active_tickers = [stock['ticker'] for stock in active_stocks_data["top_stocks"]]
    price_lookup = {stock['ticker']: stock.get('close_price') for stock in active_stocks_data["top_stocks"]}
    
    print(f"--- Found active stocks: {active_tickers} ---")
    print("--- Filtering for optionable stocks... ---")

    # Step 2: Filter for optionable stocks
    optionable_tickers = []
    for ticker in active_tickers:
        details = _get_ticker_details(ticker)
        # Check if the 'results' key and the 'options' key exist and if 'optionable' is true
        if details and details.get('results', {}).get('options', {}).get('optionable'):
            optionable_tickers.append(ticker)
        else:
            print(f"--- Skipping {ticker} (not optionable) ---")
    
    print(f"--- Found optionable stocks: {optionable_tickers} ---")
    
    # Step 3: Concurrently analyze the filtered list of stocks
    final_results = []
    for ticker in optionable_tickers:
        print(f"--- Analyzing {ticker}... ---")
        try:
            analysis_str, news_str = await asyncio.gather(
                asyncio.to_thread(_get_and_analyze_ticker, ticker=ticker),
                asyncio.to_thread(_get_news_for_ticker, ticker=ticker)
            )
            final_results.append({
                "ticker": ticker,
                "price": price_lookup.get(ticker, "N/A"),
                "technical_analysis": json.loads(analysis_str),
                "news": json.loads(news_str)
            })
        except Exception as e:
            print(f"--- Failed to process {ticker}: {e} ---")
            final_results.append({
                "ticker": ticker,
                "price": price_lookup.get(ticker, "N/A"),
                "error": f"An error occurred while processing this stock: {e}"
            })
            
    return json.dumps(final_results, indent=2)

# --- Pydantic Schema for the Super-Tool ---
class FindAndAnalyzeActiveStocksInput(BaseModel):
    limit: int = Field(5, description="The number of top active stocks to analyze.")

# --- FINAL, SIMPLIFIED TOOL LIST ---
# The agent only gets ONE tool. This prevents confusion and forces it
# to use the reliable workflow we defined in Python.
tools = [
    StructuredTool.from_function(
        func=_find_and_analyze_active_stocks,
        name="find_and_analyze_top_stocks",
        description="The primary tool to get a full trading analysis. It finds the most active stocks, gets their technicals and news, and returns a complete report.",
        args_schema=FindAndAnalyzeActiveStocksInput,
        coroutine=_find_and_analyze_active_stocks # Pass the async version
    )
]