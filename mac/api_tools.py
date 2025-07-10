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
sync_client = httpx.Client(verify=False, timeout=60)

# --- Base URL Configuration ---
DATA_API_BASE_URL = "https://tda.kewar.org"
TA_API_BASE_URL = "https://tta.kewar.org"

# --- Component Functions (Synchronous for this test) ---
def _get_most_active_stocks(limit: int = 100):
    url = f"{DATA_API_BASE_URL}/most-active-stocks"
    response = sync_client.get(url, params={"limit": limit})
    response.raise_for_status()
    return response.json()

def _get_ticker_details(ticker: str):
    url = f"https://api.polygon.io/v3/reference/tickers/{ticker}"
    params = {"apiKey": os.getenv("POLYGON_API_KEY")}
    response = sync_client.get(url, params=params)
    response.raise_for_status()
    return response.json()

def _get_news_for_ticker(ticker: str, days: int = 7):
    url = f"{DATA_API_BASE_URL}/news/{ticker}"
    params = {"days": days}
    response = sync_client.get(url, params=params)
    response.raise_for_status()
    return response.json()

def _get_and_analyze_ticker(ticker: str, days: int = 90):
    hist_url = f"{DATA_API_BASE_URL}/historical-data/{ticker}"
    hist_response = sync_client.get(hist_url, params={"days": days})
    hist_response.raise_for_status()
    
    ta_url = f"{TA_API_BASE_URL}/analyze"
    ta_response = sync_client.post(ta_url, json=hist_response.json())
    ta_response.raise_for_status()
    return ta_response.json()

# --- The "Super-Tool" (Simplified for debugging) ---
async def _find_and_analyze_active_stocks(limit: int = 5) -> str:
    log.info(f"🚀 [Debug Mode] Kicking off analysis for top {limit} stocks")
    
    active_stocks_data = await asyncio.to_thread(_get_most_active_stocks, limit)
    
    if "error" in active_stocks_data or not active_stocks_data.get("top_stocks"):
        return json.dumps({"error": "Could not retrieve active stocks."})

    active_stocks = active_stocks_data["top_stocks"]
    price_lookup = {stock['ticker']: stock.get('close_price') for stock in active_stocks}
    log.info(f"Found {len(active_stocks)} active stocks. Filtering for optionable tickers...")

    optionable_tickers = []
    for stock in active_stocks:
        ticker = stock['ticker']
        try:
            details = await asyncio.to_thread(_get_ticker_details, ticker)
            
            # --- ✅ THE IMPORTANT DEBUGGING LINE ---
            if ticker == 'NVDA':
                log.info(f"DEBUG INFO FOR NVDA: {json.dumps(details, indent=2)}")

            # The original filtering logic we are testing
            if isinstance(details, dict) and details.get('results', {}).get('options', {}).get('optionable'):
                optionable_tickers.append(ticker)
        except Exception as e:
            log.error(f"Could not get details for {ticker}: {e}")

    log.info(f"Found {len(optionable_tickers)} optionable stocks: {optionable_tickers}")
    
    # We will stop here and return an empty list for this test
    return json.dumps([], indent=2)

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