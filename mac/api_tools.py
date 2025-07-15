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

# --- Reusable HTTP Client ---
async_client = httpx.AsyncClient(verify=False, timeout=120)

# --- Base URL Configuration ---
DATA_API_BASE_URL = "https://tda.kewar.org"
TA_API_BASE_URL = "https://tta.kewar.org"
OPTIONS_API_BASE_URL = "https://toa.kewar.org"

# --- Load Optionable Tickers from Local File ---
def _load_optionable_tickers() -> set:
    """Loads the set of optionable tickers from the local JSON file."""
    try:
        script_dir = os.path.dirname(__file__)
        file_path = os.path.join(script_dir, "optionable_tickers.json")
        with open(file_path, "r") as f:
            tickers = json.load(f)
            log.info(f"Successfully loaded {len(tickers)} optionable tickers from file.")
            return set(tickers)
    except Exception as e:
        log.error(f"Could not load or parse optionable_tickers.json: {e}")
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
    # Polygon allows up to 250 tickers per snapshot request
    ticker_str = ",".join(tickers)
    url = f"https://api.polygon.io/v3/snapshot?ticker.any_of={ticker_str}"
    params = {"apiKey": os.getenv("POLYGON_API_KEY")}
    return await _get_data(url, params=params)


# --- The V2 "Super-Tool" ---
async def _find_and_analyze_active_stocks(limit: int = 10) -> str:
    log.info(f"🚀 Kicking off V2 analysis for a random sample of {limit} stocks")
    
    if not OPTIONABLE_TICKER_SET:
        return json.dumps({"error": "The list of optionable tickers is empty."})

    # 1. Select a random sample of tickers from our master list
    # Ensure limit is not greater than the number of available tickers
    sample_size = min(limit, len(OPTIONABLE_TICKER_SET))
    tickers_to_analyze = random.sample(list(OPTIONABLE_TICKER_SET), sample_size)
    log.info(f"Selected random sample for analysis: {tickers_to_analyze}")
    
    # 2. Get the last known price for our sample in one batch
    price_data = await _get_prices_for_tickers(tickers_to_analyze)
    price_lookup = {
        result['ticker']: result.get('session', {}).get('close')
        for result in price_data.get('results', [])
    }

    # 3. Concurrently fetch all other required data for the analysis
    data_tasks = {
        ticker: asyncio.gather(
            _get_data(f"{TA_API_BASE_URL}/analyze", json_payload={"ticker": ticker}),
            _get_data(f"{DATA_API_BASE_URL}/options-chain/{ticker}"),
            _get_data(f"{DATA_API_BASE_URL}/news/{ticker}"),
            # Add calls for earnings and dividends here if desired
        ) for ticker in tickers_to_analyze
    }
    
    all_results = await asyncio.gather(*data_tasks.values())
    results_map = dict(zip(data_tasks.keys(), all_results))
    
    # 4. Assemble the final report, calling the options_api for the final analysis step
    final_report = []
    for ticker, res in results_map.items():
        tech_analysis, options_chain, news = res
        stock_price = price_lookup.get(ticker)

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
            "volatility_analysis": volatility_analysis
        })

    return json.dumps(final_report, indent=2)


# --- Pydantic Schema and Tool Definition ---
class FindAndAnalyzeActiveStocksInput(BaseModel):
    limit: int = Field(10, description="The number of random optionable stocks to analyze.")

tools = [
    StructuredTool.from_function(
        func=_find_and_analyze_active_stocks,
        name="find_and_analyze_stocks",
        description="The primary tool to get a full trading analysis for a random sample of optionable stocks, including technicals, volatility, and skew.",
        args_schema=FindAndAnalyzeActiveStocksInput,
        coroutine=_find_and_analyze_active_stocks
    )
]