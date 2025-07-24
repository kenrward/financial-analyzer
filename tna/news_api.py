# news_api.py

import os
import logging
from flask import Flask, jsonify, request
from langchain_ollama import ChatOllama
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import JsonOutputParser
from pydantic import BaseModel, Field
from typing import List, Dict
import json
import asyncio

app = Flask(__name__)

# --- Configuration ---
OLLAMA_BASE_URL = "https://mmo.kewar.org" 
OLLAMA_MODEL = "llama3.1" 

# --- Setup Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Pydantic Model for a SINGLE Stock's Analysis ---
# We simplify the model to what one LLM call should return
class NewsAnalysis(BaseModel):
    sentiment_score: float = Field(description="A score from -1.0 (very bearish) to 1.0 (very bullish).")
    summary: str = Field(description="A brief, one-sentence summary of the key themes in the news.")
    justification: str = Field(description="A one-sentence justification for the assigned sentiment score.")

# --- LLM and Parser Setup for a SINGLE Analysis ---
try:
    llm = ChatOllama(model=OLLAMA_MODEL, base_url=OLLAMA_BASE_URL, temperature=0.1)
    # The parser now expects the analysis for just one stock
    parser = JsonOutputParser(pydantic_object=NewsAnalysis)

    prompt = ChatPromptTemplate.from_messages([
        ("system", "You are a financial news analyst. Your task is to analyze a list of news headlines for a single stock. Respond ONLY with a single, valid JSON object formatted according to the provided schema. Do not include any other text or explanations."),
        ("human", "Here are the news headlines for the stock:\n\n{headlines}\n\n{format_instructions}")
    ])

    # This chain is now simpler and more reliable
    chain = (prompt | llm | parser).with_retry(stop_after_attempt=2)
except Exception as e:
    logging.error(f"Failed to initialize LLM chain: {e}")
    chain = None

# --- ✅ V3 FIX: Asynchronous worker function for a single ticker ---
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
@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({"status": "healthy", "service": "news-api"}), 200

@app.route('/analyze-news-batch', methods=['POST'])
def analyze_news_batch():
    """
    Receives a batch of news, runs analysis for each ticker concurrently,
    and returns the aggregated results.
    """
    payload = request.get_json()
    if not payload or not isinstance(payload, dict):
        return jsonify({"error": "Invalid request payload. Requires a JSON object mapping tickers to headline lists."}), 400

    async def run_concurrent_analysis():
        tasks = {
            ticker: analyze_single_ticker(ticker, headlines) 
            for ticker, headlines in payload.items()
        }
        results = await asyncio.gather(*tasks.values())
        return dict(zip(tasks.keys(), results))

    try:
        logging.info(f"Analyzing news for {len(payload)} tickers concurrently...")
        # Run the async functions from our synchronous Flask endpoint
        final_results = asyncio.run(run_concurrent_analysis())
        logging.info("Successfully generated batch news analysis.")
        return jsonify(final_results), 200
    except Exception as e:
        logging.error(f"Error during batch news analysis: {e}", exc_info=True)
        return jsonify({"error": str(e), "message": "Failed to perform batch news analysis."}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5003, debug=False)