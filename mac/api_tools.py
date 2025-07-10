# api_tools.py

import asyncio
import json
import logging
import os
import httpx
from langchain.tools import StructuredTool
from pydantic import BaseModel, Field

log = logging.getLogger(__name__)

# --- Reusable HTTP Clients ---
sync_client = httpx.Client(verify=False, timeout=60)
async_client = httpx.AsyncClient(verify=False, timeout=60)

# --- Base URL Configuration ---
DATA_API_BASE_URL = "https://tda.kewar.org"
TA_API_BASE_URL = "https://tta.kewar.org"

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

async def _get_ticker_details(ticker: str):
    url = f"https://api.polygon.io/v3/reference/tickers/{ticker}"
    params = {"apiKey": os.getenv("POLYGON_API_KEY")}
    try:
        response = await async_client.get(url, params=params)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        log.error(f"Failed to get details for {ticker}: {e}")
        return {"error": f"Failed for {ticker}"}

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
    # This function orchestrates calls to the local sync APIs
    try:
        # Get historical data
        hist_url = f"{DATA_API_BASE_URL}/historical-data/{ticker}"
        hist_params = {"days": days}
        hist_response = sync_client.get(hist_url, params=hist_params)
        hist_response.raise_for_status()
        historical_data = hist_response.json()
        
        # Get technical analysis
        ta_url = f"{TA_API_BASE_URL}/analyze"
        ta_response = sync_client.post(ta_url, json=historical_data)
        ta_response.raise_for_status()
        return ta_response.json()
    except Exception as e:
        log.error(f"Failed analysis for {ticker}: {e}")
        return {"error": f"Failed TA for {ticker}"}

# --- The "Super-Tool" - Fully Asynchronous and Optimized ---
async def _find_and_analyze_active_stocks(limit: int = 5) -> str:
    log.info(f"🚀 Kicking off full analysis for top {limit} stocks")
    
    active_stocks_data = await _get_most_active_stocks(limit)
    if "error" in active_stocks_data or not active_stocks_data.get("top_stocks"):
        return json.dumps({"error": "Could not retrieve active stocks."})

    active_stocks = active_stocks_data["top_stocks"]
    price_lookup = {stock['ticker']: stock.get('close_price') for stock in active_stocks}
    log.info(f"Found {len(active_stocks)} active stocks. Filtering for optionable tickers...")

    details_tasks = [_get_ticker_details(stock['ticker']) for stock in active_stocks]
    details_results = await asyncio.gather(*details_tasks, return_exceptions=True)

    optionable_tickers = [
        active_stocks[i]['ticker']
        for i, details in enumerate(details_results)
        if isinstance(details, dict) and details.get('results', {}).get('options', {}).get('optionable')
    ]
    log.info(f"Found {len(optionable_tickers)} optionable stocks: {optionable_tickers}")

    analysis_tasks = {ticker: _get_and_analyze_ticker(ticker) for ticker in optionable_tickers}
    news_tasks = {ticker: _get_news_for_ticker(ticker) for ticker in optionable_tickers}
    
    analysis_results = await asyncio.gather(*analysis_tasks.values(), return_exceptions=True)
    news_results = await asyncio.gather(*news_tasks.values(), return_exceptions=True)
    
    analysis_map = {ticker: res for ticker, res in zip(analysis_tasks.keys(), analysis_results)}
    news_map = {ticker: res for ticker, res in zip(news_tasks.keys(), news_results)}

    final_results = []
    for ticker in optionable_tickers:
        final_results.append({
            "ticker": ticker,
            "price": price_lookup.get(ticker, "N/A"),
            "technical_analysis": analysis_map.get(ticker, {"error": "Analysis failed"}),
            "news": news_map.get(ticker, {"error": "News fetch failed"})
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