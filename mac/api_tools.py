# api_tools.py
import asyncio
import json
import logging
import os
import httpx
from langchain.tools import StructuredTool
from pydantic import BaseModel, Field

log = logging.getLogger(__name__)

# Reusable HTTP Client
async_client = httpx.AsyncClient(verify=False, timeout=60)

# Base URL Configuration
DATA_API_BASE_URL = "https://tda.kewar.org"
TA_API_BASE_URL = "https://tta.kewar.org"
OPTIONS_API_BASE_URL = "https://toa.kewar.org"

# Load Optionable Tickers from Local File
def _load_optionable_tickers() -> set:
    try:
        script_dir = os.path.dirname(__file__)
        file_path = os.path.join(script_dir, "optionable_tickers.json")
        with open(file_path, "r") as f:
            return set(json.load(f))
    except Exception as e:
        log.error(f"Could not load optionable_tickers.json: {e}")
        return set()

OPTIONABLE_TICKER_SET = _load_optionable_tickers()


# --- Component Functions for API Calls ---
async def _get_data(url: str, params: dict = None, json_payload: dict = None):
    """Generic data fetching helper."""
    try:
        if json_payload:
            response = await async_client.post(url, json=json_payload)
        else:
            response = await async_client.get(url, params=params)
        response.raise_for_status()
        return response.json()
    except httpx.HTTPStatusError as e:
        return {"error": f"HTTP Error: {e.response.status_code}", "message": e.response.text}
    except Exception as e:
        return {"error": "Request Failed", "message": str(e)}

# --- The V2 "Super-Tool" ---
async def _find_and_analyze_active_stocks(limit: int = 5) -> str:
    log.info(f"🚀 Kicking off V2 analysis for top {limit} stocks")
    
    active_stocks_data = await _get_data(f"{DATA_API_BASE_URL}/most-active-stocks", params={"limit": limit})
    if "error" in active_stocks_data:
        return json.dumps(active_stocks_data)

    active_stocks = active_stocks_data.get("top_stocks", [])
    price_lookup = {stock['ticker']: stock.get('close_price') for stock in active_stocks}
    
    optionable_tickers = [s['ticker'] for s in active_stocks if s['ticker'] in OPTIONABLE_TICKER_SET]
    log.info(f"Found {len(optionable_tickers)} optionable stocks to analyze: {optionable_tickers}")

    if not optionable_tickers:
        return json.dumps([])

    initial_data_tasks = {
        ticker: asyncio.gather(
            _get_data(f"{TA_API_BASE_URL}/analyze", json_payload={"ticker": ticker}),
            _get_data(f"{DATA_API_BASE_URL}/options-chain/{ticker}"),
            _get_data(f"{DATA_API_BASE_URL}/news/{ticker}")
        ) for ticker in optionable_tickers
    }
    
    initial_results = await asyncio.gather(*initial_data_tasks.values())
    results_map = dict(zip(initial_data_tasks.keys(), initial_results))
    
    final_report = []
    for ticker, res in results_map.items():
        tech_analysis, options_chain, news = res
        volatility_analysis = {}

        # ✅ --- THE FIX: More specific error checking ---
        # Check each dependency before proceeding.
        if "error" in tech_analysis:
            log.warning(f"TA failed for {ticker}: {tech_analysis.get('message')}")
            volatility_analysis = {"error": "Technical analysis data was unavailable."}
        elif "error" in options_chain:
            log.warning(f"Options chain failed for {ticker}: {options_chain.get('message')}")
            volatility_analysis = {"error": "Options chain data was unavailable."}
        else:
            # If both dependencies are good, call the volatility analysis service
            payload = {
                "ticker": ticker,
                "stock_price": price_lookup.get(ticker),
                "options_chain": options_chain.get("options_chain", []),
                "historical_volatility": tech_analysis.get("indicators", {}).get("HV_30D_Annualized")
            }
            volatility_analysis = await _get_data(f"{OPTIONS_API_BASE_URL}/analyze-volatility", json_payload=payload)

        final_report.append({
            "ticker": ticker,
            "price": price_lookup.get(ticker, "N/A"),
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