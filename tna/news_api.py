# news_api.py (FastAPI Version)

import os
import logging
from fastapi import FastAPI, HTTPException
from langchain_ollama import ChatOllama
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import JsonOutputParser
from pydantic import BaseModel, Field
from typing import List, Dict
import json
import asyncio

# --- FastAPI App Initialization ---
app = FastAPI()

# --- Configuration ---
OLLAMA_BASE_URL = "https://mmo.kewar.org" 
OLLAMA_MODEL = "llama3.1" 

# --- Setup Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Pydantic Models for Structured Output ---
class NewsAnalysis(BaseModel):
    sentiment_score: float = Field(description="A score from -1.0 (very bearish) to 1.0 (very bullish).")
    summary: str = Field(description="A brief, one-sentence summary of the key themes in the news.")
    justification: str = Field(description="A one-sentence justification for the assigned sentiment score.")

# --- LLM and Parser Setup ---
try:
    llm = ChatOllama(model=OLLAMA_MODEL, base_url=OLLAMA_BASE_URL, temperature=0.1)
    parser = JsonOutputParser(pydantic_object=NewsAnalysis)

    prompt = ChatPromptTemplate.from_messages([
        ("system", "You are a financial news analyst. Your task is to analyze a list of news headlines for a single stock. Respond ONLY with a single, valid JSON object formatted according to the provided schema. Do not include any other text or explanations."),
        ("human", "Here are the news headlines for the stock:\n\n{headlines}\n\n{format_instructions}")
    ])

    chain = (prompt | llm | parser).with_retry(stop_after_attempt=2)
except Exception as e:
    logging.error(f"Failed to initialize LLM chain: {e}")
    chain = None

# --- Asynchronous worker function for a single ticker ---
async def analyze_single_ticker(ticker: str, headlines: List[str]):
    """Analyzes news for one ticker and returns the result."""
    if not chain:
        return {"error": "LLM chain not available"}
    
    headlines_text = "\n".join(f"- {h}" for h in headlines)
    try:
        analysis_result = await chain.ainvoke({
            "headlines": headlines_text,
            "format_instructions": parser.get_format_instructions()
        })
        return analysis_result
    except Exception as e:
        logging.error(f"LLM call failed for ticker {ticker}: {e}")
        return {"error": f"LLM analysis failed for {ticker}"}

# --- API Endpoints ---
@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "news-api"}

@app.post("/analyze-news-batch")
async def analyze_news_batch(payload: Dict[str, List[str]]):
    """
    Receives a batch of news, runs analysis for each ticker concurrently,
    and returns the aggregated results. This is now a native async endpoint.
    """
    if not payload:
        raise HTTPException(status_code=400, detail="Invalid request payload. Requires a JSON object mapping tickers to headline lists.")

    try:
        logging.info(f"Analyzing news for {len(payload)} tickers concurrently...")
        tasks = {
            ticker: analyze_single_ticker(ticker, headlines) 
            for ticker, headlines in payload.items()
        }
        results = await asyncio.gather(*tasks.values())
        final_results = dict(zip(tasks.keys(), results))
        logging.info("Successfully generated batch news analysis.")
        return final_results
    except Exception as e:
        logging.error(f"Error during batch news analysis: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to perform batch news analysis.")

# To run this with uvicorn:
# uvicorn news_api:app --host 0.0.0.0 --port 5003