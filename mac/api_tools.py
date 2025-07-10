# api_tools.py

import asyncio
import json
import logging
import os
import httpx
from langchain.tools import StructuredTool
from pydantic import BaseModel, Field
from polygon import RESTClient # Import the official Polygon client

log = logging.getLogger(__name__)

# --- Reusable HTTP and Polygon Clients ---
async_client = httpx.AsyncClient(verify=False, timeout=60)
# Create an instance of the official Polygon client
polygon_client = RESTClient(os.getenv("POLYGON_API_KEY"))

# --- Base URL Configuration ---
DATA_API_BASE_URL = "https://tda.kewar.org"
TA_API_BASE_URL = "https://tta.kewar.org"


# --- ✅ The New, More Reliable Optionable Check ---
def _check_options_sync(ticker: str) -> bool:
    """Synchronous helper to check for option contracts."""
    try:
        # We only need to know if at least ONE contract exists. limit=1 is a key optimization.
        contracts = polygon_client.list_options_contracts(underlying_ticker=ticker, limit=1)
        # The client returns an iterator. We try to get the first item.
        # If it succeeds, options exist. If it raises StopIteration, the list is empty.
        next(contracts)
        return True
    except StopIteration:
        # This is the expected result for a stock with no options
        return False
    except Exception as e:
        log.error(f"Polygon client error checking options for {ticker}: {e}")
        return False

async def _is_ticker_optionable(ticker: str) -> bool:
    """Asynchronously checks if a ticker is optionable."""
    return await asyncio.to_thread(_check_options_sync, ticker)


# --- Component Functions (Unchanged) ---
async def _get_most_active_stocks(limit: int = 100):
    url = f"{DATA_API_BASE_URL}/most-active-stocks"
    try:
        response = await async_client.get(url, params={"limit": limit})
        response.raise_for_status()
        return response.json()
    except Exception as e:
        log.error(f"Failed to get active stocks: {e}")
        return {"error": "Failed to get active stocks"}

async def _get_news_for_ticker(ticker: str, days: int = 7):
    # ... (function is unchanged)
    url = f"{DATA_API_BASE_URL}/news/{ticker}"
    params = {"days": days}
    try:
        response = await async_client.get(url, params=params)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        log.error(f"Failed to get news for {ticker}: {e}")
        return {"error": f"Failed news for {ticker}"}
        
async def _get_and_analyze_ticker(ticker: str, days: int = 90):
    # ... (function is unchanged)
    try:
        async with httpx.AsyncClient(verify=False) as session:
            hist_url = f"{DATA_API_BASE_URL}/historical-data/{ticker}"
            hist_response = await session.get(hist_url, params={"days": days})
            hist_response.raise_for_status()
            
            ta_url = f"{TA_API_BASE_URL}/analyze"
            ta_response = await session.post(ta_url, json=hist_response.json())
            ta_response.raise_for_status()
            return ta_response.json()
    except Exception as e:
        log.error(f"Failed analysis for {ticker}: {e}")
        return {"error": f"Failed TA for {ticker}"}


# --- The "Super-Tool" (Now using the new optionable check) ---
async def _find_and_analyze_active_stocks(limit: int = 5) -> str:
    log.info(f"🚀 Kicking off full analysis for top {limit} stocks")
    
    active_stocks_data = await _get_most_active_stocks(limit)
    if "error" in active_stocks_data or not active_stocks_data.get("top_stocks"):
        return json.dumps({"error": "Could not retrieve active stocks."})

    active_stocks = active_stocks_data["top_stocks"]
    price_lookup = {stock['ticker']: stock.get('close_price') for stock in active_stocks}
    log.info(f"Found {len(active_stocks)} active stocks. Filtering for optionable tickers...")

    # Concurrently check for optionability using the new, reliable method
    optionable_tasks = {stock['ticker']: _is_ticker_optionable(stock['ticker']) for stock in active_stocks}
    optionable_results = await asyncio.gather(*optionable_tasks.values())
    
    optionable_tickers = [ticker for ticker, is_optionable in zip(optionable_tasks.keys(), optionable_results) if is_optionable]
    log.info(f"Found {len(optionable_tickers)} optionable stocks: {optionable_tickers}")

    # ... (The rest of the function to analyze and fetch news remains the same) ...
    analysis_tasks = {ticker: _get_and_analyze_ticker(ticker) for ticker in optionable_tickers}
    news_tasks = {ticker: _get_news_for_ticker(ticker) for ticker in optionable_tickers}
    
    analysis_results = await asyncio.gather(*analysis_tasks.values())
    news_results = await asyncio.gather(*news_tasks.values())
    
    analysis_map = {ticker: res for ticker, res in zip(analysis_tasks.keys(), analysis_results)}
    news_map = {ticker: res for ticker, res in zip(news_tasks.keys(), news_results)}

    final_results = []
    for ticker in optionable_tickers:
        final_results.append({
            "ticker": ticker,
            "price": price_lookup.get(ticker, "N/A"),
            "technical_analysis": analysis_map.get(ticker),
            "news": news_map.get(ticker)
        })

    return json.dumps(final_results, indent=2)


# --- Pydantic Schema and Tool Definition (Unchanged) ---
class FindAndAnalyzeActiveStocksInput(BaseModel):
    limit: int = Field(5, description="The number of top active stocks to analyze.")

tools = [
    StructuredTool.from_function(
        func=_find_and_analyze_active_stocks,
        name="find_and_analyze_top_stocks",
        description="The primary tool to get a full trading analysis for optionable stocks.",
        args_schema=FindAndAnalyzeActiveStocksInput,
        coroutine=_find_and_analyze_active_stocks
    )
]