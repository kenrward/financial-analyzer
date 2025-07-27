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

# --- The V3 "Super-Tool" ---
async def analyze_specific_tickers(tickers_to_analyze: List[str]) -> str:
    log.info(f"🚀 Kicking off V3 analysis for {len(tickers_to_analyze)} stocks: {tickers_to_analyze}")
    if not tickers_to_analyze:
        return json.dumps({"error": "No tickers provided."})

    # 1. Get prices first
    price_data = await _get_prices_for_tickers(tickers_to_analyze)
    price_lookup = {
        res['ticker']: res.get('session', {}).get('close')
        for res in price_data.get('results', [])
        if res.get('session') and res.get('session').get('close') is not None
    }

    # 2. Concurrently fetch TA, options chain, and RAW news
    initial_data_tasks = {
        ticker: asyncio.gather(
            _get_data(f"{TA_API_BASE_URL}/analyze", json_payload={"ticker": ticker}),
            _get_data(f"{DATA_API_BASE_URL}/options-chain/{ticker}"),
            _get_data(f"{DATA_API_BASE_URL}/news/{ticker}"),
            _get_data(f"{DATA_API_BASE_URL}/dividends/{ticker}"),
            _get_data(f"{DATA_API_BASE_URL}/earnings-calendar/{ticker}"),
        ) for ticker in tickers_to_analyze
    }
    
    all_results = await asyncio.gather(*initial_data_tasks.values())
    results_map = dict(zip(initial_data_tasks.keys(), all_results))
    vix_context = await _get_data(f"{TA_API_BASE_URL}/analyze-index/I:VIX")
    
    # ✅ 3. V3: Prepare and send news for BATCH analysis in smaller chunks
    news_batch_payload = {}
    for ticker, res in results_map.items():
        _, _, news_data, _, _ = res
        if isinstance(news_data, dict) and "news" in news_data:
            news_batch_payload[ticker] = [article['title'] for article in news_data['news']]

    analyzed_news = {}
    CHUNK_SIZE = 10 # Process 10 stocks at a time
    ticker_chunks = [list(news_batch_payload.keys())[i:i + CHUNK_SIZE] for i in range(0, len(news_batch_payload), CHUNK_SIZE)]

    for chunk in ticker_chunks:
        log.info(f"Sending news chunk of {len(chunk)} tickers to batch analysis service...")
        chunk_payload = {ticker: news_batch_payload[ticker] for ticker in chunk}
        chunk_result = await _get_data(f"{NEWS_API_BASE_URL}/analyze-news-batch", json_payload=chunk_payload)
        if isinstance(chunk_result, dict):
            analyzed_news.update(chunk_result)
    
    # 4. Assemble the final report
    final_report = []
    for ticker, res in results_map.items():
        tech_analysis, options_chain, _, dividends, earnings = res
        stock_price = price_lookup.get(ticker)
        
        volatility_analysis = {}
        if "error" in tech_analysis or "error" in options_chain or stock_price is None:
            volatility_analysis = {"error": "Missing data for volatility analysis."}
        else:
            payload = {
                "ticker": ticker, "stock_price": stock_price,
                "options_chain": options_chain.get("options_chain", []),
                "historical_volatility": tech_analysis.get("indicators", {}).get("HV_30D_Annualized")
            }
            volatility_analysis = await _get_data(f"{OPTIONS_API_BASE_URL}/analyze-volatility", json_payload=payload)

        final_report.append({
            "ticker": ticker, "price": stock_price,
            "news_analysis": analyzed_news.get(ticker, {"error": "News analysis failed or was not available."}),
            "dividends": dividends, "earnings": earnings,
            "technical_analysis": tech_analysis,
            "volatility_analysis": volatility_analysis,
            "market_context": {"vix_rank": vix_context.get("52_week_rank_percent")}
        })

    return json.dumps(final_report, indent=2)