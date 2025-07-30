# api_tools.py

import asyncio
import json
import logging
import os
import random
import httpx
from typing import List

log = logging.getLogger(__name__)

# --- Reusable HTTP Client ---
async_client = httpx.AsyncClient(verify=False, timeout=120)

# --- Configuration ---
DATA_API_BASE_URL = "https://tda.kewar.org"
TA_API_BASE_URL = "https://tta.kewar.org"
OPTIONS_API_BASE_URL = "https://toa.kewar.org"
ANALYSIS_SEMAPHORE = asyncio.Semaphore(8)

# --- Helper Functions ---
async def _make_request(url: str, json_payload: dict = None, params: dict = None):
    """The actual request-making logic."""
    try:
        if json_payload:
            response = await async_client.post(url, json=json_payload, timeout=120)
        else:
            response = await async_client.get(url, params=params, timeout=120)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        log.error(f"Request Failed for {url}: {e}")
        return {"error": "Request Failed", "message": str(e)}

async def _get_data(url: str, json_payload: dict = None, params: dict = None):
    """Generic data fetching helper that respects the semaphore for our backend services."""
    if "kewar.org" in url:
        async with ANALYSIS_SEMAPHORE:
            return await _make_request(url, json_payload, params)
    else:
        # For external APIs like Polygon, we don't use our internal semaphore.
        return await _make_request(url, json_payload, params)

async def _get_prices_for_tickers(tickers: list):
    """Uses the Unified Snapshot to get the last price for a list of tickers."""
    ticker_str = ",".join(tickers)
    url = f"https://api.polygon.io/v3/snapshot?ticker.any_of={ticker_str}"
    params = {"apiKey": os.getenv("POLYGON_API_KEY")}
    return await _get_data(url, params=params)

# --- ✅ V3: New helper function to process a single ticker's full data pipeline ---
async def _gather_data_for_ticker(ticker: str, price_lookup: dict, vix_context: dict):
    """
    Orchestrates the entire data gathering pipeline for a single stock.
    This function does NOT call any LLMs.
    """
    log.info(f"Gathering data for ticker: {ticker}")
    
    # 1. Fetch initial data concurrently
    tech_analysis, options_chain, news_data, dividends, earnings = await asyncio.gather(
        _get_data(f"{TA_API_BASE_URL}/analyze", json_payload={"ticker": ticker}),
        _get_data(f"{DATA_API_BASE_URL}/options-chain/{ticker}"),
        _get_data(f"{DATA_API_BASE_URL}/news/{ticker}"),
        _get_data(f"{DATA_API_BASE_URL}/dividends/{ticker}"),
        _get_data(f"{DATA_API_BASE_URL}/earnings-calendar/{ticker}"),
    )
    
    # 2. Analyze volatility (no LLM involved)
    stock_price = price_lookup.get(ticker)
    volatility_analysis = {}
    if "error" in tech_analysis or "error" in options_chain or stock_price is None:
        volatility_analysis = {"error": "Missing critical data for volatility analysis."}
    else:
        payload = {
            "ticker": ticker, "stock_price": stock_price,
            "options_chain": options_chain.get("options_chain", []),
            "historical_volatility": tech_analysis.get("indicators", {}).get("HV_30D_Annualized")
        }
        volatility_analysis = await _get_data(f"{OPTIONS_API_BASE_URL}/analyze-volatility", json_payload=payload)

    # 3. Assemble and return the final raw data object for this ticker
    return {
        "ticker": ticker, "price": stock_price,
        "raw_news": news_data.get("news", []), # Return the raw news list
        "dividends": dividends, "earnings": earnings,
        "technical_analysis": tech_analysis,
        "volatility_analysis": volatility_analysis,
        "market_context": {"vix_rank": vix_context.get("52_week_rank_percent")}
    }

# --- The V3 "Super-Tool" ---
async def analyze_specific_tickers(tickers_to_analyze: List[str]) -> str:
    """
    The main data gathering function. It orchestrates all backend API calls
    to assemble a complete raw data package for the LLM to analyze.
    """
    log.info(f"🚀 Kicking off V3 data gathering for {len(tickers_to_analyze)} stocks.")
    if not tickers_to_analyze:
        return json.dumps({"error": "No tickers provided."})

    # 1. Get prices and VIX context once for the entire run
    price_data = await _get_prices_for_tickers(tickers_to_analyze)
    price_lookup = {
        res['ticker']: res.get('session', {}).get('close')
        for res in price_data.get('results', [])
        if res.get('session') and res.get('session').get('close') is not None
    }
    vix_context = await _get_data(f"{TA_API_BASE_URL}/analyze-index/I:VIX")

    # 2. Create and run the full data gathering pipeline for all tickers concurrently
    analysis_tasks = [
        _gather_data_for_ticker(ticker, price_lookup, vix_context) 
        for ticker in tickers_to_analyze
    ]
    
    final_report_data = await asyncio.gather(*analysis_tasks)
    
    return json.dumps(final_report_data, indent=2)