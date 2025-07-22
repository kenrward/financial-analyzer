# news_api.py

import os
import logging
from flask import Flask, jsonify, request
from langchain_ollama import ChatOllama
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import JsonOutputParser
from pydantic import BaseModel, Field # ✅ V3: Updated import from Pydantic v2
from typing import List

app = Flask(__name__)

# --- Configuration ---
OLLAMA_BASE_URL = "http://192.168.86.67:11434"
OLLAMA_MODEL = "llama3.1" 

# --- Setup Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Pydantic Model for Structured Output ---
# This class definition is the same, just the import source has changed.
class NewsAnalysis(BaseModel):
    sentiment_score: float = Field(description="A score from -1.0 (very bearish) to 1.0 (very bullish).")
    summary: str = Field(description="A brief, one-sentence summary of the key themes in the news.")
    justification: str = Field(description="A one-sentence justification for the assigned sentiment score.")

# --- LLM and Parser Setup ---
try:
    llm = ChatOllama(model=OLLAMA_MODEL, base_url=OLLAMA_BASE_URL, temperature=0.1)
    parser = JsonOutputParser(pydantic_object=NewsAnalysis)

    prompt = ChatPromptTemplate.from_messages([
        ("system", "You are a financial news analyst. Your task is to analyze a list of news headlines for a given stock. Provide a concise summary, a sentiment score, and a justification. Respond with a JSON object formatted according to the provided schema."),
        ("human", "Here are the news headlines for the stock:\n\n{headlines}\n\n{format_instructions}")
    ])

    chain = prompt | llm | parser
except Exception as e:
    logging.error(f"Failed to initialize LLM chain: {e}")
    chain = None

# --- API Endpoints ---
@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({"status": "healthy", "service": "news-api"}), 200

@app.route('/analyze-news', methods=['POST'])
def analyze_news():
    """
    Receives a list of news headlines, uses an LLM to analyze them,
    and returns a structured JSON object with the analysis.
    """
    if not chain:
        return jsonify({"error": "LLM analysis chain is not available."}), 503

    payload = request.get_json()
    headlines_list = payload.get('headlines')

    if not headlines_list or not isinstance(headlines_list, list):
        return jsonify({"error": "Invalid request payload. Requires a 'headlines' key with a list of strings."}), 400

    headlines_text = "\n".join(f"- {h}" for h in headlines_list)

    try:
        logging.info(f"Analyzing {len(headlines_list)} headlines...")
        analysis_result = chain.invoke({
            "headlines": headlines_text,
            "format_instructions": parser.get_format_instructions()
        })
        logging.info("Successfully generated news analysis.")
        return jsonify(analysis_result), 200

    except Exception as e:
        logging.error(f"Error during news analysis LLM call: {e}", exc_info=True)
        return jsonify({"error": str(e), "message": "Failed to perform news analysis."}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5003, debug=True)
