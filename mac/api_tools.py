# api_tools.py

import asyncio
import json
import logging
import os
import httpx
from langchain.tools import StructuredTool
from pydantic import BaseModel, Field
from polygon import RESTClient

log = logging.getLogger(__name__)

# --- Reusable HTTP and Polygon Clients ---
async_client = httpx.AsyncClient(verify=False, timeout=60)
polygon_client = RESTClient(os.getenv("POLYGON_API_KEY"))

# --- Concurrency Limiter ---
POLYGON_API_SEMAPHORE = asyncio.Semaphore(10)

# --- Base URL Configuration ---
DATA_API_BASE_URL = "https://tda.kewar.org"
TA_API_BASE_URL = "https://tta.kewar.org"
OPTIONS_API_BASE_URL = "https://toa.kewar.org"

# --- ✅ V2: Enhanced Optionable Check with Detailed Logging ---
def _check_options_sync(ticker: str) -> tuple[bool, str]:
    """
    Synchronous helper that checks for option contracts.
    Returns a tuple: (is_optionable, reason_string)
    """
    try:
        contracts = polygon_client.list_options_contracts(underlying_ticker=ticker, limit=1)
        next(contracts)
        return (True, "Has Options")
    except StopIteration:
        return (False, "No options found")
    except Exception as e:
        # Capture the specific error from the API client
        error_message = str(e)
        log.error(f"Polygon client error checking options for {ticker}: {error_message}")
        return (False, f"API Error: {error_message}")

async def _is_ticker_optionable(ticker: str) -> tuple[bool, str]:
    """Asynchronously checks if a ticker is optionable, respecting the semaphore."""
    async with POLYGON_API_SEMAPHORE:
        return await asyncio.to_thread(_check_options_sync, ticker)


# --- Component Functions (Unchanged) ---
async def _get_most_active_stocks(limit: int = 100):
    url = f"{DATA_API_BASE_URL}/most-active-stocks"
    # ... (rest of function is unchanged)
    
# ... (other helper functions like _get_news, _get_ta_analysis, etc. are unchanged) ...


# --- The V2 "Super-Tool" with Enhanced Logging ---
async def _find_and_analyze_active_stocks(limit: int = 5) -> str:
    log.info(f"🚀 Kicking off V2 analysis for top {limit} stocks")
    
    active_stocks_data = await _get_most_active_stocks(limit)
    active_stocks = active_stocks_data.get("top_stocks", [])
    price_lookup = {stock['ticker']: stock.get('close_price') for stock in active_stocks}
    log.info(f"Found {len(active_stocks)} active stocks. Filtering for optionable tickers...")

    # Concurrently check for optionability
    optionable_tasks = {stock['ticker']: _is_ticker_optionable(stock['ticker']) for stock in active_stocks}
    optionable_results = await asyncio.gather(*optionable_tasks.values())
    
    optionable_tickers = []
    # Loop through the results to provide detailed logging
    for ticker, result_tuple in zip(optionable_tasks.keys(), optionable_results):
        is_optionable, reason = result_tuple
        if is_optionable:
            optionable_tickers.append(ticker)
        else:
            # This will print the exact reason for skipping
            log.warning(f"Skipping {ticker} (Reason: {reason})")
    
    log.info(f"Found {len(optionable_tickers)} optionable stocks to analyze: {optionable_tickers}")

    if not optionable_tickers:
        return json.dumps([])

    # ... (The rest of the function remains the same) ...
    # It will now proceed to analyze only the tickers that passed the filter.
    
    return json.dumps({"message": "Analysis would continue here..."}, indent=2) # Placeholder return for the test

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