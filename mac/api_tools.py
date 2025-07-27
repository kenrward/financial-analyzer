# api_tools.py

import asyncio
import json
import logging
import os
import random
import httpx
from typing import List, Dict
from langchain_ollama import ChatOllama
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import JsonOutputParser
from pydantic import BaseModel, Field

log = logging.getLogger(__name__)

# --- Reusable HTTP Client ---
async_client = httpx.AsyncClient(verify=False, timeout=120)

# --- Configuration ---
DATA_API_BASE_URL = "https://tda.kewar.org"
TA_API_BASE_URL = "https://tta.kewar.org"
OPTIONS_API_BASE_URL = "https://toa.kewar.org"
ANALYSIS_SEMAPHORE = asyncio.Semaphore(8)

# --- V3: LLM Configuration for Local News Analysis ---
OLLAMA_BASE_URL = "http://localhost:11434" 
OLLAMA_MODEL = "llama3.1" 

# --- Pydantic Models for News Analysis ---
class NewsAnalysis(BaseModel):
    sentiment_score: float = Field(description="A score from -1.0 (very bearish) to 1.0 (very bullish).")
    summary: str = Field(description="A brief, one-sentence summary of the key themes in the news.")
    justification: str = Field(description="A one-sentence justification for the assigned sentiment score.")

# --- LLM and Parser Setup for News Analysis ---
try:
    news_llm = ChatOllama(model=OLLAMA_MODEL, base_url=OLLAMA_BASE_URL, temperature=0.1)
    news_parser = JsonOutputParser(pydantic_object=NewsAnalysis)
    news_prompt = ChatPromptTemplate.from_messages([
        ("system", "You are a financial news analyst. Analyze the provided headlines for a single stock. Respond ONLY with a valid JSON object matching the requested format. Do not include any other text."),
        ("human", "Headlines:\n\n{headlines}\n\n{format_instructions}")
    ])
    news_analysis_chain = (news_prompt | news_llm | news_parser).with_retry(stop_after_attempt=2)
except Exception as e:
    log.error(f"Failed to initialize local LLM chain for news analysis: {e}")
    news_analysis_chain = None

# --- Helper Functions ---
async def _get_data(url: str, json_payload: dict = None, params: dict = None):
    """Generic data fetching helper that respects the semaphore for our backend services."""
    async with ANALYSIS_SEMAPHORE:
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

async def _analyze_news_locally(ticker: str, headlines: List[str]):
    """Analyzes news for one ticker using the local LLM."""
    if not news_analysis_chain:
        return {"error": "News analysis LLM chain not available"}
    
    headlines_text = "\n".join(f"- {h}" for h in headlines)
    try:
        return await news_analysis_chain.ainvoke({
            "headlines": headlines_text,
            "format_instructions": news_parser.get_format_instructions()
        })
    except Exception as e:
        log.error(f"Local LLM call failed for ticker {ticker}: {e}")
        return {"error": f"Local LLM news analysis failed for {ticker}"}

# --- The V3 "Super-Tool" ---
async def analyze_specific_tickers(tickers_to_analyze: List[str]) -> str:
    log.info(f"🚀 Kicking off V3 analysis for {len(tickers_to_analyze)} stocks: {tickers_to_analyze}")
    if not tickers_to_analyze:
        return json.dumps({"error": "No tickers provided."})

    # 1. Concurrently fetch all raw data from backend services
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
    
    # 2. Prepare and concurrently analyze news LOCALLY
    news_analysis_tasks = {}
    for ticker, res in results_map.items():
        _, _, news_data, _, _ = res
        if isinstance(news_data, dict) and "news" in news_data:
            headlines = [article['title'] for article in news_data['news']]
            news_analysis_tasks[ticker] = _analyze_news_locally(ticker, headlines)

    log.info(f"Analyzing news for {len(news_analysis_tasks)} tickers locally...")
    analyzed_news_results = await asyncio.gather(*news_analysis_tasks.values())
    analyzed_news_map = dict(zip(news_analysis_tasks.keys(), analyzed_news_results))
    
    # 3. Assemble the final report
    final_report = []
    for ticker, res in results_map.items():
        tech_analysis, options_chain, _, dividends, earnings = res
        stock_price = tech_analysis.get("indicators", {}).get("last_close") if isinstance(tech_analysis, dict) else None
        
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
            "news_analysis": analyzed_news_map.get(ticker, {"error": "News analysis not performed."}),
            "dividends": dividends, "earnings": earnings,
            "technical_analysis": tech_analysis,
            "volatility_analysis": volatility_analysis,
            "market_context": {"vix_rank": vix_context.get("52_week_rank_percent")}
        })

    return json.dumps(final_report, indent=2)
