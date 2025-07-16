# api_tools.py

import asyncio
import json
import logging
import os
import random
import httpx
import pandas as pd
from langchain.tools import StructuredTool
from pydantic import BaseModel, Field

log = logging.getLogger(__name__)

# --- Reusable HTTP Client ---
async_client = httpx.AsyncClient(verify=False, timeout=120)

# --- Configuration ---
DATA_API_BASE_URL = "https://tda.kewar.org"
TA_API_BASE_URL = "https://tta.kewar.org"
OPTIONS_API_BASE_URL = "https://toa.kewar.org"
MASTER_PARQUET_PATH = "/mnt/shared-drive/us_stocks_daily.parquet"

# --- Load Optionable Tickers from Local File ---
def _load_optionable_tickers() -> set:
    """Loads the set of optionable tickers from the local JSON file."""
    try:
        script_dir = os.path.dirname(__file__)
        file_path = os.path.join(script_dir, "optionable_stocks.json")
        with open(file_path, "r") as f:
            tickers = json.load(f)
            log.info(f"Successfully loaded {len(tickers)} optionable tickers from file.")
            return set(tickers)
    except Exception as e:
        log.error(f"Could not load or parse optionable_stocks.json: {e}")
        return set()

OPTIONABLE_TICKER_SET = _load_optionable_tickers()


# --- Helper function for pre-filtering tickers by price ---
def _get_prefiltered_tickers(min_price: float) -> list:
    """
    Loads the master parquet file, gets the last price for each ticker,
    applies the price filter, and returns a list of eligible, optionable tickers.
    """
    log.info(f"Reading local database to pre-filter tickers by price >= ${min_price}...")
    try:
        df = pd.read_parquet(MASTER_PARQUET_PATH, columns=['ticker', 'date', 'close'])
        # Find the last closing price for each ticker
        last_prices = df.sort_values('date').groupby('ticker')['close'].last()
        
        # Filter by price
        price_filtered_tickers = last_prices[last_prices >= min_price].index.tolist()
        
        # Intersect with our master optionable list to get the final list
        final_eligible_tickers = list(set(price_filtered_tickers) & OPTIONABLE_TICKER_SET)
        
        log.info(f"Found {len(final_eligible_tickers)} optionable tickers matching price criteria.")
        return final_eligible_tickers
    except Exception as e:
        log.error(f"Failed to pre-filter tickers from local data: {e}")
        return []

# --- Generic helper for making API calls ---
async def _get_data(url: str, json_payload: dict = None):
    """Generic data fetching helper."""
    try:
        if json_payload:
            response = await async_client.post(url, json=json_payload, timeout=120)
        else:
            response = await async_client.get(url, timeout=120)
        response.raise_for_status()
        return response.json()
    except httpx.HTTPStatusError as e:
        return {"error": f"HTTP Error: {e.response.status_code}", "message": e.response.text}
    except Exception as e:
        return {"error": "Request Failed", "message": str(e)}

# --- The V2 "Super-Tool" with Pre-filtering ---
async def _find_and_analyze_active_stocks(limit: int = 5, min_price: float = 0.0) -> str:
    log.info(f"🚀 Kicking off V2 analysis for {limit} stocks with min price ${min_price}")
    
    # 1. Get the pre-filtered list of tickers that meet our criteria
    eligible_tickers = await asyncio.to_thread(_get_prefiltered_tickers, min_price)

    if not eligible_tickers:
        return json.dumps({"error": "No optionable tickers found matching the price criteria."})

    # 2. Select a random sample from the pre-filtered list
    sample_size = min(limit, len(eligible_tickers))
    tickers_to_analyze = random.sample(eligible_tickers, sample_size)
    log.info(f"Selected random sample for analysis: {tickers_to_analyze}")
    
    # 3. Concurrently fetch all data for the analysis
    initial_data_tasks = {
        ticker: asyncio.gather(
            _get_data(f"{TA_API_BASE_URL}/analyze", json_payload={"ticker": ticker}),
            _get_data(f"{DATA_API_BASE_URL}/options-chain/{ticker}"),
            _get_data(f"{DATA_API_BASE_URL}/news/{ticker}"),
            _get_data(f"{DATA_API_BASE_URL}/dividends/{ticker}"),
            _get_data(f"{DATA_API_BASE_URL}/earnings-calendar/{ticker}"),
            return_exceptions=True
        ) for ticker in tickers_to_analyze
    }
    
    all_results = await asyncio.gather(*initial_data_tasks.values())
    results_map = dict(zip(initial_data_tasks.keys(), all_results))
    vix_context = await _get_data(f"{TA_API_BASE_URL}/analyze-index/I:VIX")
    
    # 4. Assemble the final report, calling the options_api for the final analysis step
    final_report = []
    for ticker, res in results_map.items():
        tech_analysis, options_chain, news, dividends, earnings = res
        
        # Get the reliable price from our technical analysis data
        stock_price = tech_analysis.get("indicators", {}).get("last_close") if isinstance(tech_analysis, dict) else None
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
            "dividends": dividends,
            "earnings": earnings,
            "technical_analysis": tech_analysis,
            "volatility_analysis": volatility_analysis,
            "market_context": {"vix_rank": vix_context.get("52_week_rank_percent")}
        })

    return json.dumps(final_report, indent=2)


# --- Pydantic Schema and Tool Definition ---
class FindAndAnalyzeStocksInput(BaseModel):
    limit: int = Field(5, description="The number of random optionable stocks to analyze.")
    min_price: float = Field(10.0, description="The minimum stock price to consider for analysis.")

tools = [
    StructuredTool.from_function(
        func=_find_and_analyze_active_stocks,
        name="find_and_analyze_stocks",
        description="The primary tool to get a full trading analysis for a random sample of optionable stocks, including technicals, volatility, and skew.",
        args_schema=FindAndAnalyzeStocksInput,
        coroutine=_find_and_analyze_active_stocks
    )
]