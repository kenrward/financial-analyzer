# api_tools.py

import asyncio
import json
import logging
import os
import random
import httpx
from langchain.tools import StructuredTool
from pydantic import BaseModel, Field

log = logging.getLogger(__name__)

# Reusable HTTP Client
async_client = httpx.AsyncClient(verify=False, timeout=120)

# Base URL Configuration
DATA_API_BASE_URL = "https://tda.kewar.org"
TA_API_BASE_URL = "https://tta.kewar.org"
OPTIONS_API_BASE_URL = "https://toa.kewar.org"

# --- ✅ CORRECTED: Load Optionable Tickers from Complex JSON File ---
def _load_optionable_tickers() -> set:
    """
    Loads the list of optionable ticker objects from the local JSON file
    and extracts just the ticker symbols.
    """
    try:
        script_dir = os.path.dirname(__file__)
        file_path = os.path.join(script_dir, "optionable_stocks.json")
        
        log.info(f"Attempting to load optionable tickers from: {file_path}")
        with open(file_path, "r") as f:
            # Load the list of dictionary objects
            data_objects = json.load(f)
            # Use a list comprehension to extract the 'ticker' value from each object
            tickers = [item['ticker'] for item in data_objects if 'ticker' in item]
            
            log.info(f"Successfully loaded and extracted {len(tickers)} tickers from file.")
            # Using a set provides very fast lookups
            return set(tickers)
    except FileNotFoundError:
        log.warning(f"optionable_stocks.json not found at {file_path}. No stocks will be filtered.")
        return set()
    except Exception as e:
        log.error(f"Could not load or parse optionable_stocks.json: {e}")
        return set()

OPTIONABLE_TICKER_SET = _load_optionable_tickers()


# --- Component Functions for API Calls ---
async def _get_data(url: str, params: dict = None, json_payload: dict = None):
    """Generic data fetching helper."""
    try:
        if json_payload:
            response = await async_client.post(url, json=json_payload, timeout=120)
        else:
            response = await async_client.get(url, params=params, timeout=120)
        response.raise_for_status()
        return response.json()
    except httpx.HTTPStatusError as e:
        return {"error": f"HTTP Error: {e.response.status_code}", "message": e.response.text}
    except Exception as e:
        return {"error": "Request Failed", "message": str(e)}

async def _get_prices_for_tickers(tickers: list):
    """Uses the Unified Snapshot to get the last price for a list of tickers."""
    ticker_str = ",".join(tickers)
    url = f"https://api.polygon.io/v3/snapshot?ticker.any_of={ticker_str}"
    params = {"apiKey": os.getenv("POLYGON_API_KEY")}
    return await _get_data(url, params=params)


# --- The V2 "Super-Tool" ---
async def _find_and_analyze_active_stocks(limit: int = 5, min_price: float = 0.0) -> str:
    log.info(f"🚀 Kicking off V2 analysis for a random sample of {limit} stocks")
    
    if not OPTIONABLE_TICKER_SET:
        return json.dumps({"error": "The list of optionable tickers is empty or could not be loaded."})

    # 1. Select a random sample of tickers from our master list
    sample_size = min(limit, len(OPTIONABLE_TICKER_SET))
    initial_tickers = random.sample(list(OPTIONABLE_TICKER_SET), sample_size)
    log.info(f"Selected random initial sample: {initial_tickers}")
    
    # 2. Concurrently fetch all the data needed for analysis AND filtering
    initial_data_tasks = {
        ticker: asyncio.gather(
            _get_data(f"{TA_API_BASE_URL}/analyze", json_payload={"ticker": ticker}),
            _get_data(f"{DATA_API_BASE_URL}/options-chain/{ticker}"),
            _get_data(f"{DATA_API_BASE_URL}/news/{ticker}"),
            # Add calls for earnings and dividends here
        ) for ticker in initial_tickers
    }
    
    all_results = await asyncio.gather(*initial_data_tasks.values())
    results_map = dict(zip(initial_data_tasks.keys(), all_results))
    vix_context = await _get_data(f"{TA_API_BASE_URL}/analyze-index/I:VIX")
    
    # ✅ 3. Filter the results based on price from the TA data
    final_report = []
    log.info("Filtering results by minimum price...")
    for ticker, res in results_map.items():
        tech_analysis, options_chain, news = res
        
        # Get the reliable last close price from our own data
        last_price = tech_analysis.get("indicators", {}).get("last_close")
        
        # Skip if price is below min or if TA failed
        if last_price is None or last_price < min_price:
            log.warning(f"Skipping {ticker} (Price: {last_price}) - does not meet min price of ${min_price}.")
            continue
        
        # 4. If the price is good, proceed with volatility analysis
        volatility_analysis = {}
        if "error" in options_chain:
            volatility_analysis = {"error": "Options chain data was unavailable."}
        else:
            payload = {
                "ticker": ticker,
                "stock_price": last_price,
                "options_chain": options_chain.get("options_chain", []),
                "historical_volatility": tech_analysis.get("indicators", {}).get("HV_30D_Annualized")
            }
            volatility_analysis = await _get_data(f"{OPTIONS_API_BASE_URL}/analyze-volatility", json_payload=payload)

        final_report.append({
            "ticker": ticker,
            "price": last_price,
            "news": news,
            "technical_analysis": tech_analysis,
            "volatility_analysis": volatility_analysis,
            "market_context": {"vix_rank": vix_context.get("52_week_rank_percent")}
        })

    log.info(f"Analysis complete. Found {len(final_report)} stocks matching all criteria.")
    return json.dumps(final_report, indent=2)


# --- Pydantic Schema and Tool Definition ---
class FindAndAnalyzeStocksInput(BaseModel):
    limit: int = Field(5, description="The number of random optionable stocks to analyze.")

tools = [
    StructuredTool.from_function(
        func=_find_and_analyze_active_stocks,
        name="find_and_analyze_stocks",
        description="The primary tool to get a full trading analysis for a random sample of optionable stocks, including technicals, volatility, and skew.",
        args_schema=FindAndAnalyzeStocksInput,
        coroutine=_find_and_analyze_active_stocks
    )
]