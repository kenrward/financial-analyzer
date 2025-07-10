# api_tools.py

import asyncio
import json
import logging
import os
import httpx
from langchain.tools import StructuredTool
from pydantic import BaseModel, Field

# NOTE: We are removing the 'polygon' library dependency to fix the connection pool issue.
# from polygon import RESTClient 

log = logging.getLogger(__name__)

# --- Reusable HTTP Client ---
async_client = httpx.AsyncClient(verify=False, timeout=60)

# --- ⚙️ Concurrency Limiter (Semaphore) ---
# This will ensure no more than 10 concurrent requests to the Polygon API
POLYGON_API_SEMAPHORE = asyncio.Semaphore(10)

# --- Base URL Configuration ---
DATA_API_BASE_URL = "https://tda.kewar.org"
TA_API_BASE_URL = "https://tta.kewar.org"


# --- ✅ NEW: Reliable Optionable Check using httpx ---
async def _is_ticker_optionable(ticker: str) -> bool:
    """Checks if a ticker is optionable using httpx and a semaphore."""
    url = "https://api.polygon.io/v3/reference/options/contracts"
    params = {
        "underlying_ticker": ticker,
        "limit": 1,
        "apiKey": os.getenv("POLYGON_API_KEY")
    }
    
    async with POLYGON_API_SEMAPHORE:
        try:
            response = await async_client.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            # If the results list is not empty, it means an options contract was found.
            return len(data.get("results", [])) > 0
        except Exception as e:
            log.error(f"Could not check optionability for {ticker}: {e}")
            return False


# --- Component Functions ---
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

# --- The "Super-Tool" ---
async def _find_and_analyze_active_stocks(limit: int = 5) -> str:
    log.info(f"🚀 Kicking off full analysis for top {limit} stocks")
    
    active_stocks_data = await _get_most_active_stocks(limit)
    if "error" in active_stocks_data or not active_stocks_data.get("top_stocks"):
        return json.dumps({"error": "Could not retrieve active stocks."})

    active_stocks = active_stocks_data["top_stocks"]
    price_lookup = {stock['ticker']: stock.get('close_price') for stock in active_stocks}
    log.info(f"Found {len(active_stocks)} active stocks. Filtering for optionable tickers...")

    # Concurrently check for optionability using our new reliable, semaphore-controlled function
    optionable_tasks = {stock['ticker']: _is_ticker_optionable(stock['ticker']) for stock in active_stocks}
    optionable_results = await asyncio.gather(*optionable_tasks.values())
    
    optionable_tickers = [ticker for ticker, is_optionable in zip(optionable_tasks.keys(), optionable_results) if is_optionable]
    log.info(f"Found {len(optionable_tickers)} optionable stocks: {optionable_tickers}")

    if not optionable_tickers:
        return json.dumps([], indent=2)

    # Concurrently fetch analysis and news for the filtered list
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


# --- Pydantic Schema and Tool Definition ---
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