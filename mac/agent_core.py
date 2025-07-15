# agent_core.py

import nest_asyncio
nest_asyncio.apply()

import asyncio
import json
import logging
from langchain_ollama import ChatOllama
from langchain_core.messages import HumanMessage

from api_tools import _find_and_analyze_active_stocks

# --- ⚙️ Set up Logging ---
logging.basicConfig(
    level=logging.DEBUG, # Changed to DEBUG to see more detail
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

    # STEP 1: Directly call the data gathering function
    logging.info("STEP 1: Directly executing data gathering and analysis tool...")
    raw_data_json_string = await _find_and_analyze_active_stocks(limit)

    if not raw_data_json_string:
        logging.error("❗️ Tool execution did not produce a final output string.")
        return

    logging.info("STEP 1 Complete: Raw data successfully retrieved.")

    # --- ✅ DEBUGGING STEP ---
    # Log the raw string from the tool and the object after parsing
    logging.debug(f"RAW JSON STRING FROM TOOL:\n{raw_data_json_string}")
    # --- End of Debugging Step ---

    # STEP 2: Iteratively Synthesize the data
    logging.info("STEP 2: Starting iterative synthesis of the report...")
    try:
        results_list = json.loads(raw_data_json_string)

        # --- ✅ DEBUGGING STEP ---
        logging.debug(f"PARSED RESULTS LIST:\n{results_list}")
        if results_list:
            logging.debug(f"TYPE OF FIRST ITEM IN LIST: {type(results_list[0])}")
        # --- End of Debugging Step ---

        if not results_list:
            # ... (rest of function)
            return

    except json.JSONDecodeError as e:
        # ... (rest of function)
        return

    # Print the markdown table header
    print("\n\n--- FINAL REPORT ---")
    print("| Ticker | Price | Outlook | Justification |")
    print("| :--- | :--- | :--- | :--- |")

    for stock_data in results_list:
        # This is where the original error occurred
        single_stock_prompt = f"""
        You are a financial analyst... The data is: {json.dumps(stock_data)}
        ...
        """
        logging.info(f"Synthesizing report for: {stock_data.get('ticker')}")
        response = await llm.ainvoke(single_stock_prompt)
        table_row = response.content.strip().replace("'", "")
        print(table_row)

    logging.info("✅ Workflow Finished!")


# --- Main Execution Block ---
if __name__ == '__main__':
    logging.info("Agent starting up...")
    NUM_STOCKS_TO_ANALYZE = 5 # Using a smaller number for faster debugging
    asyncio.run(run_trading_analysis_workflow(limit=NUM_STOCKS_TO_ANALYZE))