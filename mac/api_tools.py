# api_tools.py

import asyncio
import json
import logging
import os
import httpx
from langchain.tools import StructuredTool
from pydantic import BaseModel, Field
from typing import List

log = logging.getLogger(__name__)

# Reusable HTTP Client
async_client = httpx.AsyncClient(verify=False, timeout=120)

# Base URL Configuration
DATA_API_BASE_URL = "https://tda.kewar.org"
TA_API_BASE_URL = "https://tta.kewar.org"
OPTIONS_API_BASE_URL = "https://toa.kewar.org"

# --- Component Functions for API Calls ---
async def _get_data(url: str, json_payload: dict = None):
    """Generic data fetching helper."""
    try:
        if json_payload:
            response = await async_client.post(url, json=json_payload, timeout=120)
        else:
            response = await async_client.get(url, timeout=120)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        return {"error": "Request Failed", "message": str(e)}

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

    # 1. Get the last known price for the provided list
    price_data = await _get_prices_for_tickers(tickers_to_analyze)
    price_lookup = {
        result['ticker']: result.get('session', {}).get('close')
        for result in price_data.get('results', [])
        if result.get('session') and result.get('session').get('close') is not None
    }

    # 2. Concurrently fetch all other required data
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

tools = [
    StructuredTool.from_function(
        func=analyze_specific_tickers,
        name="analyze_stocks",
        description="The primary tool to get a full trading analysis for a specific list of stocks, including technicals and volatility.",
        args_schema=AnalyzeTickersInput,
        coroutine=analyze_specific_tickers
    )
]