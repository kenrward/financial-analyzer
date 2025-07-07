import httpx
from langchain.tools import Tool # Import Tool class directly
import json
from datetime import date, timedelta

# --- Configuration ---
DATA_API_BASE_URL = "https://tda.kewar.org"
TA_API_BASE_URL = "https://tta.kewar.org"
http_client = httpx.Client(verify=False)

def _make_api_call(url: str, method: str = "GET", params: dict = None, json_data: dict = None):
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

# --- Raw functions (not decorated yet) ---
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

# --- LangChain Tool Instances ---
# We define the tools by explicitly creating Tool instances.
# The functions passed to Tool are the _raw_ functions without @tool decorator.
get_most_active_stocks_tool = Tool(
    name="get_most_active_stocks",
    description="""
    Fetches a list of the most active stocks by trading volume for the previous trading day.
    Use this tool to identify stocks with high liquidity and interest.
    Input is an integer `limit` for the number of top stocks to retrieve (default is 100).
    Returns a JSON string of a list of stock tickers and their daily summary.
    Example: '[{"ticker": "NVDA", "volume": 172017167.0, ...}, {"ticker": "AAPL", ...}]'
    """,
    func=_get_most_active_stocks # Pass the raw function
)

get_historical_data_tool = Tool(
    name="get_historical_data",
    description="""
    Retrieves historical daily OHLCV (Open, High, Low, Close, Volume) data for a given stock ticker.
    Use this to get data needed for technical analysis.
    Input is the stock ticker symbol (e.g., "AAPL") and optionally the number of historical days.
    Returns a JSON string of historical data.
    Example: '{"ticker": "AAPL", "data": [{"date": "2025-06-20", "open": 150.0, ...}, ...]}'
    """,
    func=_get_historical_data
)

get_news_for_ticker_tool = Tool(
    name="get_news_for_ticker",
    description="""
    Fetches recent news articles for a given stock ticker.
    Use this to gather qualitative information and sentiment for a stock.
    Input is the stock ticker symbol (e.g., "NVDA") and optionally the number of recent days to look back.
    Returns a JSON string of news articles.
    Example: '{"ticker": "NVDA", "news": [{"title": "NVIDIA stock up", "publisher": "Reuters", ...}, ...]}'
    """,
    func=_get_news_for_ticker
)

analyze_technical_patterns_tool = Tool(
    name="analyze_technical_patterns",
    description="""
    Sends historical OHLCV data for a stock to the Technical Analysis API
    to identify common technical indicators (RSI, MACD, SMAs) and simple patterns
    like SMA crossovers.
    Input requires the stock ticker symbol (e.g., "MSFT") and the historical OHLCV data
    as a JSON string (e.g., from get_historical_data tool's output).
    The historical_data_json should contain a 'ticker' key and a 'data' key
    where 'data' is a list of dictionaries with 'date', 'open', 'high', 'low', 'close', 'volume'.
    Returns a JSON string of the analysis results, including indicators and detected patterns.
    Example: '{"ticker": "MSFT", "patterns": ["SMA Crossover: ..."], "indicators": {"RSI": 65.2, ...}}'
    """,
    func=_analyze_technical_patterns
)

# We provide a list of tool objects that the agent can use
# This list will be imported by agent_core.py
tools = [
    get_most_active_stocks_tool,
    get_historical_data_tool,
    get_news_for_ticker_tool,
    analyze_technical_patterns_tool
]

# --- Test functions (for debugging, won't be used by LLM directly) ---
def test_tools():
    print(f"Data API URL: {DATA_API_BASE_URL}")
    print(f"TA API URL: {TA_API_BASE_URL}")

    print("\n--- Testing get_most_active_stocks_tool (limit=5) ---")
    # Call the invoke method on the Tool object
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