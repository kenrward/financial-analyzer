# api_tools.py

import asyncio
import json
import logging
import os
import httpx
from langchain.tools import StructuredTool
from pydantic import BaseModel, Field

log = logging.getLogger(__name__)

# --- Reusable HTTP Client ---
async_client = httpx.AsyncClient(verify=False, timeout=60)

# --- Base URL Configuration ---
DATA_API_BASE_URL = "https://tda.kewar.org"
TA_API_BASE_URL = "https://tta.kewar.org"


# --- ✅ NEW: Load Optionable Tickers from Local File ---
def _load_optionable_tickers() -> set:
    """Loads the set of optionable tickers from a local JSON file."""
    try:
        # Assumes the file is in the same directory as this script
        with open("optionable_tickers.json", "r") as f:
            tickers = json.load(f)
            # Using a set provides very fast lookups
            return set(tickers)
    except FileNotFoundError:
        log.warning("optionable_tickers.json not found. No stocks will be filtered as optionable.")
        return set()
    except json.JSONDecodeError:
        log.error("Could not parse optionable_tickers.json. Please check its format.")
        return set()

# Load the set of tickers once when the module is imported
OPTIONABLE_TICKER_SET = _load_optionable_tickers()


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

# --- The "Super-Tool" - Now with Local File Filtering ---
async def _find_and_analyze_active_stocks(limit: int = 5) -> str:
    log.info(f"🚀 Kicking off full analysis for top {limit} stocks")
    
    active_stocks_data = await _get_most_active_stocks(limit)
    if "error" in active_stocks_data or not active_stocks_data.get("top_stocks"):
        return json.dumps({"error": "Could not retrieve active stocks."})

    active_stocks = active_stocks_data["top_stocks"]
    price_lookup = {stock['ticker']: stock.get('close_price') for stock in active_stocks}
    log.info(f"Found {len(active_stocks)} active stocks. Filtering against local list...")

    # ✅ Filter using the fast, in-memory set from the local file
    optionable_tickers = [
        stock['ticker'] 
        for stock in active_stocks 
        if stock['ticker'] in OPTIONABLE_TICKER_SET
    ]
    log.info(f"Found {len(optionable_tickers)} optionable stocks in local list: {optionable_tickers}")

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