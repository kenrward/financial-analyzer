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

app = Flask(__name__)

# --- Configuration ---
# ✅ V3: Updated to use the new Traefik URL for Ollama
OLLAMA_BASE_URL = "https://mmo.kewar.org" 
OLLAMA_MODEL = "llama3.1" 

# --- Setup Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Pydantic Models for Structured Output ---
class NewsAnalysis(BaseModel):
    sentiment_score: float = Field(description="A score from -1.0 (very bearish) to 1.0 (very bullish).")
    summary: str = Field(description="A brief, one-sentence summary of the key themes in the news.")
    justification: str = Field(description="A one-sentence justification for the assigned sentiment score.")

class BatchNewsAnalysis(BaseModel):
    results: Dict[str, NewsAnalysis] = Field(description="A dictionary mapping ticker symbols to their news analysis.")


# --- LLM and Parser Setup ---
try:
    llm = ChatOllama(model=OLLAMA_MODEL, base_url=OLLAMA_BASE_URL, temperature=0.1)
    parser = JsonOutputParser(pydantic_object=BatchNewsAnalysis)

    prompt = ChatPromptTemplate.from_messages([
        ("system", "You are an efficient financial news analyst. Your task is to analyze news headlines for multiple stocks provided in a single JSON object. For each stock, provide a concise summary, a sentiment score, and a justification. Respond with a single JSON object where the main key is 'results', which contains a dictionary mapping each ticker symbol to its analysis object, formatted according to the provided schema."),
        ("human", "Here is the JSON object containing news headlines for multiple stocks:\n\n{headlines_json}\n\n{format_instructions}")
    ])

    chain = prompt | llm | parser
except Exception as e:
    logging.error(f"Failed to initialize LLM chain: {e}")
    chain = None

# --- API Endpoints ---
@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({"status": "healthy", "service": "news-api"}), 200

@app.route('/analyze-news-batch', methods=['POST'])
def analyze_news_batch():
    """
    Receives a batch of news headlines for multiple tickers, uses an LLM to analyze them
    in a single call, and returns a structured JSON object with the combined analysis.
    """
    if not chain:
        return jsonify({"error": "LLM analysis chain is not available."}), 503

    payload = request.get_json()

    if not payload or not isinstance(payload, dict):
        return jsonify({"error": "Invalid request payload. Requires a JSON object mapping tickers to headline lists."}), 400

    try:
        logging.info(f"Analyzing news for {len(payload)} tickers in a single batch...")
        analysis_result = chain.invoke({
            "headlines_json": json.dumps(payload, indent=2),
            "format_instructions": parser.get_format_instructions()
        })
        logging.info("Successfully generated batch news analysis.")
        return jsonify(analysis_result.get('results', {})), 200

    except Exception as e:
        logging.error(f"Error during batch news analysis LLM call: {e}", exc_info=True)
        return jsonify({"error": str(e), "message": "Failed to perform batch news analysis."}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5003, debug=True)