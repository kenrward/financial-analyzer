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
OPTIONS_API_BASE_URL = "https://toa.kewar.org" # ✅ V2: URL for the new options service

# --- Load Optionable Tickers from Local File (Unchanged) ---
def _load_optionable_tickers() -> set:
    try:
        script_dir = os.path.dirname(__file__)
        file_path = os.path.join(script_dir, "optionable_tickers.json")
        log.info(f"Attempting to load optionable tickers from: {file_path}")
        with open(file_path, "r") as f:
            tickers = json.load(f)
            log.info(f"Successfully loaded {len(tickers)} tickers from file.")
            return set(tickers)
    except Exception as e:
        log.error(f"Could not load or parse optionable_tickers.json: {e}")
        return set()

OPTIONABLE_TICKER_SET = _load_optionable_tickers()


# --- Component Functions for API Calls ---
async def _get_most_active_stocks(limit: int = 100):
    url = f"{DATA_API_BASE_URL}/most-active-stocks"
    response = await async_client.get(url, params={"limit": limit})
    response.raise_for_status()
    return response.json()

async def _get_news(ticker: str):
    url = f"{DATA_API_BASE_URL}/news/{ticker}"
    response = await async_client.get(url)
    response.raise_for_status()
    return response.json()

async def _get_ta_analysis(ticker: str):
    """Gets all technical analysis, including Historical Volatility."""
    url = f"{TA_API_BASE_URL}/analyze"
    response = await async_client.post(url, json={"ticker": ticker})
    response.raise_for_status()
    return response.json()

async def _get_options_chain(ticker: str):
    """Gets the full options chain with IV and greeks."""
    url = f"{DATA_API_BASE_URL}/options-chain/{ticker}"
    response = await async_client.get(url)
    response.raise_for_status()
    return response.json()

async def _get_volatility_analysis(payload: dict):
    """Sends data to the new options_api for skew and spread analysis."""
    url = f"{OPTIONS_API_BASE_URL}/analyze-volatility"
    response = await async_client.post(url, json=payload)
    response.raise_for_status()
    return response.json()


# --- The V2 "Super-Tool" ---
async def _find_and_analyze_active_stocks(limit: int = 5) -> str:
    log.info(f"🚀 Kicking off V2 analysis for top {limit} stocks")
    
    # 1. Get active stocks and filter against our local list
    active_stocks_data = await _get_most_active_stocks(limit)
    active_stocks = active_stocks_data.get("top_stocks", [])
    price_lookup = {stock['ticker']: stock.get('close_price') for stock in active_stocks}
    
    optionable_tickers = [s['ticker'] for s in active_stocks if s['ticker'] in OPTIONABLE_TICKER_SET]
    log.info(f"Found {len(optionable_tickers)} optionable stocks to analyze: {optionable_tickers}")

    if not optionable_tickers:
        return json.dumps([])

    # 2. Concurrently fetch all required data for the analysis
    tasks = {
        ticker: {
            "tech_analysis": _get_ta_analysis(ticker),
            "options_chain": _get_options_chain(ticker),
            "news": _get_news(ticker)
        } for ticker in optionable_tickers
    }
    
    results = {}
    for ticker, funcs in tasks.items():
        results[ticker] = await asyncio.gather(*funcs.values(), return_exceptions=True)

    # 3. Assemble the final report, calling the options_api for the final analysis step
    final_report = []
    for ticker in optionable_tickers:
        tech_analysis, options_chain, news = results[ticker]
        
        # Check for errors in the fetched data
        if isinstance(tech_analysis, Exception) or isinstance(options_chain, Exception):
            log.error(f"Skipping volatility analysis for {ticker} due to data fetching error.")
            # Still append what we have
            final_report.append({"ticker": ticker, "price": price_lookup.get(ticker), "news": news, "technical_analysis": tech_analysis, "volatility_analysis": {"error": "Missing data for analysis."}})
            continue

        # Prepare payload for the new options_api service
        volatility_payload = {
            "ticker": ticker,
            "stock_price": price_lookup.get(ticker),
            "options_chain": options_chain.get("options_chain", []),
            "historical_volatility": tech_analysis.get("indicators", {}).get("HV_30D_Annualized")
        }
        
        # Call the options_api for the final piece of analysis
        volatility_analysis = await _get_volatility_analysis(volatility_payload)

        final_report.append({
            "ticker": ticker,
            "price": price_lookup.get(ticker),
            "news": news,
            "technical_analysis": tech_analysis,
            "volatility_analysis": volatility_analysis
        })

    return json.dumps(final_report, indent=2)


# --- Pydantic Schema and Tool Definition ---
class FindAndAnalyzeActiveStocksInput(BaseModel):
    limit: int = Field(5, description="The number of top active stocks to analyze.")

tools = [
    StructuredTool.from_function(
        func=_find_and_analyze_active_stocks,
        name="find_and_analyze_top_stocks",
        description="The primary tool to get a full trading analysis for optionable stocks, including volatility and skew.",
        args_schema=FindAndAnalyzeActiveStocksInput,
        coroutine=_find_and_analyze_active_stocks
    )
]