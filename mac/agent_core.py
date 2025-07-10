# agent_core.py

import asyncio
import json
import logging
from langchain_ollama import ChatOllama

# ✅ --- THE FIX: Import the tool function directly --- ✅
from api_tools import _find_and_analyze_active_stocks

# --- ⚙️ Set up Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("agent_run.log"),
        logging.StreamHandler()
    ]
)

# --- Configuration ---
OLLAMA_BASE_URL = "http://localhost:11434"
OLLAMA_MODEL = "llama3.1" 
llm = ChatOllama(model=OLLAMA_MODEL, base_url=OLLAMA_BASE_URL, temperature=0.2)


# --- The Main Orchestration Function ---
async def run_trading_analysis_workflow(limit: int):
    logging.info(f"🚀 Kicking off Direct Execution Workflow for top {limit} stocks.")

    # --- STEP 1: Directly call the data gathering function ---
    # No agent, no LangGraph, no timeouts. Just a direct, reliable function call.
    logging.info("STEP 1: Directly executing data gathering and analysis tool...")
    raw_data_json_string = await _find_and_analyze_active_stocks(limit)

    if not raw_data_json_string:
        logging.error("❗️ Tool execution did not produce a final output string.")
        return

    logging.info("STEP 1 Complete: Raw data successfully retrieved.")
    logging.debug(f"Full data payload from tool:\n{raw_data_json_string}")

    # --- STEP 2: Iteratively Synthesize the data ---
    logging.info("STEP 2: Starting iterative synthesis of the report...")
    
    try:
        results_list = json.loads(raw_data_json_string)
        if not results_list:
            logging.warning("No optionable stocks were found to analyze.")
            print("\n\n--- FINAL REPORT ---")
            print("No optionable stocks found among the most active stocks.")
            return
    except json.JSONDecodeError as e:
        logging.error(f"❗️ Failed to parse JSON data from Step 1. Error: {e}")
        logging.error(f"--- Data that failed to parse ---:\n{raw_data_json_string}\n---")
        return

    # Print the markdown table header
    print("\n\n--- FINAL REPORT ---")
    print("| Ticker | Price | Outlook | Justification |")
    print("| :--- | :--- | :--- | :--- |")

    # Loop through each stock's data for synthesis
    for stock_data in results_list:
        single_stock_prompt = f"""
        You are a financial analyst. Your task is to analyze the data for a single stock and provide a one-line summary for a markdown table.
        The data is: {json.dumps(stock_data)}

        Determine if the outlook is Bullish, Bearish, or Neutral based on the technicals and news.
        
        Your entire response must be a single markdown table row using the format:
        | TICKER | $PRICE | Outlook | Justification |
        """
        
        logging.info(f"Synthesizing report for: {stock_data.get('ticker')}")
        response = await llm.ainvoke(single_stock_prompt)
        table_row = response.content.strip().replace("'", "")
        print(table_row)

    logging.info("✅ Workflow Finished!")


# --- Main Execution Block ---
if __name__ == '__main__':
    logging.info("Agent starting up...")
    # Define how many stocks to analyze here
    NUM_STOCKS_TO_ANALYZE = 25
    asyncio.run(run_trading_analysis_workflow(limit=NUM_STOCKS_TO_ANALYZE))