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
NEWS_API_BASE_URL = "https://tna.kewar.org"
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
        return await _make_request(url, json_payload, params)

async def _get_prices_for_tickers(tickers: list):
    """Uses the Unified Snapshot to get the last price for a list of tickers."""
    ticker_str = ",".join(tickers)
    url = f"https://api.polygon.io/v3/snapshot?ticker.any_of={ticker_str}"
    params = {"apiKey": os.getenv("POLYGON_API_KEY")}
    return await _get_data(url, params=params)

# --- V3: New helper function to process a single ticker's full pipeline ---
async def _process_single_ticker(ticker: str, price_lookup: dict, vix_context: dict):
    """
    Orchestrates the entire data gathering and analysis pipeline for a single stock.
    """
    log.info(f"Processing ticker: {ticker}")
    
    # 1. Fetch initial data concurrently
    tech_analysis, options_chain, news_data, dividends, earnings = await asyncio.gather(
        _get_data(f"{TA_API_BASE_URL}/analyze", json_payload={"ticker": ticker}),
        _get_data(f"{DATA_API_BASE_URL}/options-chain/{ticker}"),
        _get_data(f"{DATA_API_BASE_URL}/news/{ticker}"),
        _get_data(f"{DATA_API_BASE_URL}/dividends/{ticker}"),
        _get_data(f"{DATA_API_BASE_URL}/earnings-calendar/{ticker}"),
    )

    # 2. Analyze news (this now happens in a separate service)
    news_analysis = {"error": "News data not available."}
    if isinstance(news_data, dict) and "news" in news_data and news_data["news"]:
        headlines = [article['title'] for article in news_data['news']]
        news_payload = {ticker: headlines}
        # We call the batch endpoint even for a single ticker for consistency
        analyzed_news_batch = await _get_data(f"{NEWS_API_BASE_URL}/analyze-news-batch", json_payload=news_payload)
        if analyzed_news_batch and ticker in analyzed_news_batch:
            news_analysis = analyzed_news_batch[ticker]

    # 3. Analyze volatility
    stock_price = price_lookup.get(ticker)
    volatility_analysis = {}
    if "error" in tech_analysis or "error" in options_chain or stock_price is None:
        volatility_analysis = {"error": "Missing critical data (TA or Options Chain) for volatility analysis."}
    else:
        payload = {
            "ticker": ticker, "stock_price": stock_price,
            "options_chain": options_chain.get("options_chain", []),
            "historical_volatility": tech_analysis.get("indicators", {}).get("HV_30D_Annualized")
        }
        volatility_analysis = await _get_data(f"{OPTIONS_API_BASE_URL}/analyze-volatility", json_payload=payload)

    # 4. Assemble and return the final report object for this ticker
    return {
        "ticker": ticker, "price": stock_price,
        "news_analysis": news_analysis,
        "dividends": dividends, "earnings": earnings,
        "technical_analysis": tech_analysis,
        "volatility_analysis": volatility_analysis,
        "market_context": {"vix_rank": vix_context.get("52_week_rank_percent")}
    }

# --- The V3 "Super-Tool" ---
async def analyze_specific_tickers(tickers_to_analyze: List[str]) -> str:
    log.info(f"🚀 Kicking off V3 analysis for {len(tickers_to_analyze)} stocks: {tickers_to_analyze}")
    if not tickers_to_analyze:
        return json.dumps({"error": "No tickers provided."})

    # 1. Get prices for all tickers in one batch
    price_data = await _get_prices_for_tickers(tickers_to_analyze)
    price_lookup = {
        res['ticker']: res.get('session', {}).get('close')
        for res in price_data.get('results', [])
        if res.get('session') and res.get('session').get('close') is not None
    }
    
    # 2. Get VIX context once for the entire run
    vix_context = await _get_data(f"{TA_API_BASE_URL}/analyze-index/^VIX")

    # 3. Create and run the full analysis pipeline for all tickers concurrently
    analysis_tasks = [
        _process_single_ticker(ticker, price_lookup, vix_context) 
        for ticker in tickers_to_analyze
    ]
    
    final_report = await asyncio.gather(*analysis_tasks)
    
    return json.dumps(final_report, indent=2)
