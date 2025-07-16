# api_tools.py

import asyncio
import json
import logging
import os
import random
import httpx
from langchain.tools import StructuredTool
from pydantic import BaseModel, Field
from typing import List

log = logging.getLogger(__name__)

# --- Reusable HTTP Client ---
async_client = httpx.AsyncClient(verify=False, timeout=120)

# --- Configuration ---
DATA_API_BASE_URL = "https://tda.kewar.org"
TA_API_BASE_URL = "https://tta.kewar.org"
OPTIONS_API_BASE_URL = "https://toa.kewar.org"

# --- ✅ V2: Concurrency Limiter (Semaphore) ---
# This will ensure we don't send more than 8 concurrent requests to our backend services.
ANALYSIS_SEMAPHORE = asyncio.Semaphore(8)

# --- Generic helper for making API calls with semaphore ---
async def _get_data(url: str, json_payload: dict = None, params: dict = None):
    """Generic data fetching helper that respects the semaphore for analysis services."""
    # The semaphore is used for our own backend services to prevent overload.
    # We don't apply it to the external Polygon price check.
    if "kewar.org" in url:
        async with ANALYSIS_SEMAPHORE:
            return await _make_request(url, json_payload, params)
    else:
        # For external APIs like Polygon, we don't use our internal semaphore.
        return await _make_request(url, json_payload, params)

async def _make_request(url: str, json_payload: dict = None, params: dict = None):
    """The actual request-making logic."""
    try:
        if json_payload:
            response = await async_client.post(url, json=json_payload, timeout=120)
        else:
            response = await async_client.get(url, params=params, timeout=120)
        response.raise_for_status()
        return response.json()
    except httpx.HTTPStatusError as e:
        log.error(f"HTTP Error for {url}: {e.response.status_code}")
        return {"error": f"HTTP Error: {e.response.status_code}", "message": e.response.text}
    except Exception as e:
        log.error(f"Request Failed for {url}: {e}")
        return {"error": "Request Failed", "message": str(e)}

# --- Component Functions ---
async def _get_prices_for_tickers(tickers: list):
    """Uses the Unified Snapshot to get the last price for a list of tickers."""
    ticker_str = ",".join(tickers)
    url = f"https://api.polygon.io/v3/snapshot?ticker.any_of={ticker_str}"
    params = {"apiKey": os.getenv("POLYGON_API_KEY")}
    return await _get_data(url, params=params)

# --- The V2 "Super-Tool" ---
async def analyze_specific_tickers(tickers_to_analyze: List[str]) -> str:
    log.info(f"🚀 Kicking off V2 analysis for {len(tickers_to_analyze)} specific stocks: {tickers_to_analyze}")
    
    if not tickers_to_analyze:
        return json.dumps({"error": "No tickers provided for analysis."})

    # 1. Get prices first (this call is not rate-limited by our semaphore)
    price_data = await _get_prices_for_tickers(tickers_to_analyze)
    price_lookup = {
        result['ticker']: result.get('session', {}).get('close')
        for result in price_data.get('results', [])
        if result.get('session') and result.get('session').get('close') is not None
    }

    # 2. Concurrently fetch all other required data, respecting the semaphore limit
    initial_data_tasks = {
        ticker: asyncio.gather(
            _get_data(f"{TA_API_BASE_URL}/analyze", json_payload={"ticker": ticker}),
            _get_data(f"{DATA_API_BASE_URL}/options-chain/{ticker}"),
            _get_data(f"{DATA_API_BASE_URL}/news/{ticker}"),
        ) for ticker in tickers_to_analyze
    }
    
    all_results = await asyncio.gather(*initial_data_tasks.values())
    results_map = dict(zip(initial_data_tasks.keys(), all_results))
    vix_context = await _get_data(f"{TA_API_BASE_URL}/analyze-index/I:VIX")
    
    # 3. Assemble the final report
    final_report = []
    for ticker, res in results_map.items():
        tech_analysis, options_chain, news = res
        stock_price = price_lookup.get(ticker)
        
        volatility_analysis = {}
        if "error" in tech_analysis or "error" in options_chain or stock_price is None:
            volatility_analysis = {"error": "Missing data required for volatility analysis."}
        else:
            payload = {
                "ticker": ticker,
                "stock_price": stock_price,
                "options_chain": options_chain.get("options_chain", []),
                "historical_volatility": tech_analysis.get("indicators", {}).get("HV_30D_Annualized")
            }
            volatility_analysis = await _get_data(f"{OPTIONS_API_BASE_URL}/analyze-volatility", json_payload=payload)

        final_report.append({
            "ticker": ticker,
            "price": stock_price,
            "news": news,
            "technical_analysis": tech_analysis,
            "volatility_analysis": volatility_analysis,
            "market_context": {"vix_rank": vix_context.get("52_week_rank_percent")}
        })

    return json.dumps(final_report, indent=2)


# --- Pydantic Schema and Tool Definition ---
class AnalyzeTickersInput(BaseModel):
    tickers_to_analyze: List[str] = Field(..., description="A list of stock tickers to analyze.")

# Note: The tool definition is removed as it's no longer needed for direct execution.
# If you were to use this with an agent again, you would re-add the StructuredTool definition here.
