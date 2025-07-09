import httpx
from langchain.tools import StructuredTool
import json
from pydantic import BaseModel, Field

# --- Configuration (remains the same) ---
DATA_API_BASE_URL = "https://tda.kewar.org"
TA_API_BASE_URL = "https://tta.kewar.org"
http_client = httpx.Client(verify=False)

# --- Helper Functions (remain the same) ---
def _make_api_call(url: str, method: str = "GET", params: dict = None, json_data: dict = None):
    # ... (no changes to this function)
    try:
        if method == "GET":
            response = http_client.get(url, params=params, timeout=30)
        elif method == "POST":
            response = http_client.post(url, json=json_data, timeout=60)
        else:
            raise ValueError(f"Unsupported HTTP method: {method}")

        response.raise_for_status()
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


def _get_most_active_stocks(limit: int = 100) -> str:
    # ... (no changes to this function)
    url = f"{DATA_API_BASE_URL}/most-active-stocks"
    params = {"limit": limit}
    response = _make_api_call(url, params=params)
    return json.dumps(response)


def _get_historical_data(ticker: str, days: int = 90) -> str:
    # ... (no changes to this function)
    url = f"{DATA_API_BASE_URL}/historical-data/{ticker}"
    params = {"days": days}
    response = _make_api_call(url, params=params)
    return json.dumps(response)


def _get_news_for_ticker(ticker: str, days: int = 7) -> str:
    # ... (no changes to this function)
    url = f"{DATA_API_BASE_URL}/news/{ticker}"
    params = {"days": days}
    response = _make_api_call(url, params=params)
    return json.dumps(response)


def _analyze_technical_patterns(ticker: str, historical_data_json: str) -> str:
    # ... (no changes to this function)
    url = f"{TA_API_BASE_URL}/analyze"
    try:
        data_payload = json.loads(historical_data_json)
    except json.JSONDecodeError:
        return json.dumps({"error": "Invalid historical_data_json format provided to analyze_technical_patterns."})
    response = _make_api_call(url, method="POST", json_data=data_payload)
    return json.dumps(response)

# --- ✨ NEW: High-Level Combination Function ---
def _get_and_analyze_ticker(ticker: str, days: int = 90) -> str:
    """Gets historical data and immediately runs technical analysis on it."""
    print(f"--- Running combined analysis for {ticker} ---")

    # ✅ --- START OF THE FIX --- ✅
    # Enforce a minimum lookback period to ensure TA indicators can be calculated.
    if days < 50:
        print(f"--- Requested days ({days}) is less than 50. Overriding to 90. ---")
        days = 90
    # ✅ --- END OF THE FIX --- ✅

    # Step 1: Get historical data
    historical_data_str = _get_historical_data(ticker, days)
    historical_data_json = json.loads(historical_data_str)
    
    # Check if getting data failed
    if "error" in historical_data_json:
        return json.dumps(historical_data_json)
        
    # Step 2: Run technical analysis on the data we just got
    analysis_result_str = _analyze_technical_patterns(ticker, historical_data_str)
    return analysis_result_str


# --- Pydantic Schemas ---
class GetMostActiveStocksInput(BaseModel):
    limit: int = Field(100, description="The number of top stocks to retrieve.")

class GetNewsForTickerInput(BaseModel):
    ticker: str = Field(..., description="The stock ticker symbol (e.g., NVDA).")
    days: int = Field(7, description="The number of recent days to look back for news.")

# --- ✨ NEW Schema for the combined tool ---
class GetTickerAnalysisInput(BaseModel):
    ticker: str = Field(..., description="The stock ticker symbol to analyze (e.g., AAPL).")
    days: int = Field(90, description="The number of historical days to use for the analysis.")


# --- LangChain Tool Instances (Updated) ---
get_most_active_stocks_tool = StructuredTool.from_function(
    func=_get_most_active_stocks,
    name="get_most_active_stocks",
    description="Fetches a list of the most active stocks by trading volume for the previous trading day. Use this first to identify stocks of interest.",
    args_schema=GetMostActiveStocksInput
)

get_news_for_ticker_tool = StructuredTool.from_function(
    func=_get_news_for_ticker,
    name="get_news_for_ticker",
    description="Fetches recent news articles for a given stock ticker to understand sentiment and context.",
    args_schema=GetNewsForTickerInput
)

# --- ✨ NEW Combined Tool ---
get_ticker_analysis_tool = StructuredTool.from_function(
    func=_get_and_analyze_ticker,
    name="get_ticker_technical_analysis",
    description="A complete tool that gets historical data for a stock and runs a full technical analysis (RSI, MACD, SMAs). Use this for individual stock analysis.",
    args_schema=GetTickerAnalysisInput
)


# --- ✅ UPDATED: Simplified Tool List for the Agent ---
# We REMOVED the separate get_historical_data and analyze_technical_patterns tools
# to prevent the agent from getting confused.
tools = [
    get_most_active_stocks_tool,
    get_ticker_analysis_tool, # The new, powerful tool
    get_news_for_ticker_tool
]

# --- Test functions (for debugging, won't be used by LLM directly) ---
def test_tools():
    print(f"Data API URL: {DATA_API_BASE_URL}")
    print(f"TA API URL: {TA_API_BASE_URL}")

    print("\n--- Testing get_most_active_stocks_tool (limit=5) ---")
    # Call the invoke method on the Tool object, passing args as a dict
    active_stocks_response = get_most_active_stocks_tool.invoke({"limit": 5})
    print(active_stocks_response)

    # Parse to get a ticker for further tests
    try:
        active_stocks_data = json.loads(active_stocks_response)
        stocks_list = active_stocks_data.get('top_stocks', [])
    except json.JSONDecodeError:
        print("Failed to parse active stocks response.")
        stocks_list = []

    if stocks_list:
        test_ticker = stocks_list[0]['ticker']
        print(f"\n--- Testing get_historical_data_tool for {test_ticker} (60 days) ---")
        historical_data_response = get_historical_data_tool.invoke({"ticker": test_ticker, "days": 60})
        print(historical_data_response)

        print(f"\n--- Testing analyze_technical_patterns_tool for {test_ticker} ---")
        # The historical_data_response is already a JSON string, perfect for this tool's input
        ta_results_response = analyze_technical_patterns_tool.invoke({"ticker": test_ticker, "historical_data_json": historical_data_response})
        print(ta_results_response)

        print(f"\n--- Testing get_news_for_ticker_tool for {test_ticker} (3 days) ---")
        news_results_response = get_news_for_ticker_tool.invoke({"ticker": test_ticker, "days": 3})
        print(news_results_response)
    else:
        print("No active stocks found to test further tools. Skipping subsequent tests.")

if __name__ == '__main__':
    # IMPORTANT: If your homelab uses self-signed certificates, you might need to
    # configure httpx to not verify them, or add them to your system's trust store.
    # For development, you can use httpx.Client(verify=False) as shown above.
    # For production, verify=True is crucial, and proper certificates are needed.
    test_tools()