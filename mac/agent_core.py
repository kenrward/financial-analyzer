# api_tools.py

import asyncio
import json
import logging
import os
import httpx # Use httpx for both sync and async calls
from langchain.tools import StructuredTool
from pydantic import BaseModel, Field

# --- Set up logging for this module ---
log = logging.getLogger(__name__)

# --- Reusable HTTP Clients ---
# Use a context manager for the async client to ensure it's properly closed.
# The synchronous client can be module-level.
sync_client = httpx.Client(verify=False, timeout=60)

# --- Base URL Configuration ---
DATA_API_BASE_URL = "https://tda.kewar.org"
TA_API_BASE_URL = "https://tta.kewar.org"

# --- Synchronous API Call Helper (for TA API) ---
def _make_sync_api_call(url: str, method: str = "GET", params: dict = None, json_data: dict = None):
    try:
        if method == "POST":
            response = sync_client.post(url, json=json_data)
        else: # Default to GET
            response = sync_client.get(url, params=params)
        response.raise_for_status()
        return response.json()
    except httpx.RequestError as e:
        log.error(f"Sync request failed: {e}")
        return {"error": str(e)}
    except httpx.HTTPStatusError as e:
        log.error(f"Sync HTTP status error: {e.response.status_code} - {e.response.text}")
        return {"error": f"API Error: {e.response.status_code}", "message": e.response.text}

# --- Asynchronous API Call Helper (for Polygon API) ---
async def _make_async_api_call(session, url: str, params: dict = None):
    try:
        response = await session.get(url, params=params)
        response.raise_for_status()
        return response.json()
    except httpx.RequestError as e:
        log.error(f"Async request failed: {e}")
        return {"error": str(e)}
    except httpx.HTTPStatusError as e:
        log.error(f"Async HTTP status error: {e.response.status_code} - {e.response.text}")
        return {"error": f"API Error: {e.response.status_code}", "message": e.response.text}

# --- Component Functions (some now async) ---
async def _get_most_active_stocks(session, limit: int = 100):
    url = f"{DATA_API_BASE_URL}/most-active-stocks"
    return await _make_async_api_call(session, url, params={"limit": limit})

async def _get_ticker_details(session, ticker: str):
    url = f"https://api.polygon.io/v3/reference/tickers/{ticker}"
    params = {"apiKey": os.getenv("POLYGON_API_KEY")}
    return await _make_async_api_call(session, url, params=params)

async def _get_news_for_ticker(session, ticker: str, days: int = 7):
    url = f"{DATA_API_BASE_URL}/news/{ticker}"
    params = {"days": days}
    return await _make_async_api_call(session, url, params=params)

def _run_technical_analysis(ticker: str, historical_data_json: str):
    # This remains synchronous as it calls the other local service
    url = f"{TA_API_BASE_URL}/analyze"
    data_payload = json.loads(historical_data_json)
    return _make_sync_api_call(url, method="POST", json_data=data_payload)

# --- The "Super-Tool" - Fully Asynchronous and Optimized ---
async def _find_and_analyze_active_stocks(limit: int = 5) -> str:
    log.info(f"🚀 Kicking off full analysis for top {limit} stocks")
    async with httpx.AsyncClient(verify=False, timeout=60) as session:
        # Step 1: Get active stocks
        active_stocks_data = await _get_most_active_stocks(session, limit)
        if "error" in active_stocks_data or not active_stocks_data.get("top_stocks"):
            return json.dumps({"error": "Could not retrieve active stocks."})

        active_stocks = active_stocks_data["top_stocks"]
        price_lookup = {stock['ticker']: stock.get('close_price') for stock in active_stocks}
        log.info(f"Found {len(active_stocks)} active stocks. Filtering for optionable tickers...")

        # Step 2: Concurrently check for optionability
        details_tasks = [_get_ticker_details(session, stock['ticker']) for stock in active_stocks]
        details_results = await asyncio.gather(*details_tasks)

        optionable_tickers = [
            active_stocks[i]['ticker']
            for i, details in enumerate(details_results)
            if details and details.get('results', {}).get('options', {}).get('optionable')
        ]
        log.info(f"Found {len(optionable_tickers)} optionable stocks: {optionable_tickers}")

        # Step 3: Concurrently fetch data for optionable stocks
        log.info("Fetching historical data and news concurrently...")
        # Get historical data first to pass to TA
        history_tasks = {ticker: asyncio.to_thread(sync_client.get(f"{DATA_API_BASE_URL}/historical-data/{ticker}").text) for ticker in optionable_tickers}
        news_tasks = {ticker: _get_news_for_ticker(session, ticker) for ticker in optionable_tickers}
        
        history_responses = await asyncio.gather(*history_tasks.values())
        news_responses = await asyncio.gather(*news_tasks.values())
        
        history_map = {ticker: resp for ticker, resp in zip(history_tasks.keys(), history_responses)}
        news_map = {ticker: resp for ticker, resp in zip(news_tasks.keys(), news_responses)}

        # Step 4: Run TA and compile results
        log.info("Running technical analysis and compiling final report...")
        final_results = []
        for ticker in optionable_tickers:
            ta_result = await asyncio.to_thread(_run_technical_analysis, ticker, history_map[ticker])
            final_results.append({
                "ticker": ticker,
                "price": price_lookup.get(ticker, "N/A"),
                "technical_analysis": ta_result,
                "news": news_map[ticker]
            })

    return json.dumps(final_results, indent=2)

# --- Pydantic Schema and Tool Definition ---
class FindAndAnalyzeActiveStocksInput(BaseModel):
    limit: int = Field(5, description="The number of top active stocks to analyze.")

tools = [
    StructuredTool.from_function(
        func=_find_and_analyze_active_stocks,
        name="find_and_analyze_top_stocks",
        description="The primary tool to get a full trading analysis...",
        args_schema=FindAndAnalyzeActiveStocksInput,
        coroutine=_find_and_analyze_active_stocks
    )
]