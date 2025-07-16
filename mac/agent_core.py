# agent_core.py

import nest_asyncio
nest_asyncio.apply()

import asyncio
import json
import logging
import argparse

from langchain_ollama import ChatOllama
from langchain_core.messages import HumanMessage

# We import the new function directly
from api_tools import analyze_specific_tickers

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
async def run_trading_analysis_workflow(tickers: list):
    logging.info(f"🚀 Kicking off Direct Execution Workflow for tickers: {tickers}")

    # --- STEP 1: Directly call the data gathering function ---
    logging.info("STEP 1: Directly executing data analysis tool...")
    raw_data_json_string = await analyze_specific_tickers(tickers)
    
    if not raw_data_json_string:
        logging.error("❗️ Tool execution returned no data.")
        return

    logging.info("STEP 1 Complete: Raw data successfully retrieved.")
    logging.debug(f"Full data payload from tool:\n{raw_data_json_string}")

    # --- STEP 2: Iteratively Synthesize the data ---
    logging.info("STEP 2: Starting iterative synthesis of the report...")
    try:
        results_list = json.loads(raw_data_json_string)
        if isinstance(results_list, dict) and 'error' in results_list:
            logging.error(f"❗️ Tool returned an error: {results_list['error']}")
            return
        if not results_list:
            logging.warning("No stocks were analyzed.")
            return
    except json.JSONDecodeError as e:
        logging.error(f"❗️ Failed to parse JSON data from Step 1. Error: {e}")
        return

    # --- ✅ V2: Collect report lines to write to a file ---
    report_header = "| Ticker | Price | Outlook (for Premium Selling) | Justification |\n| :--- | :--- | :--- | :--- |"
    report_lines = [report_header]

    # Print the header to the console immediately
    print("\n\n--- FINAL REPORT ---")
    print(report_header)

    for stock_data in results_list:
        single_stock_prompt = f"""
        You are a senior options analyst. Your task is to analyze the following data for a single stock and provide a one-line summary for a markdown table.
        The data is: {json.dumps(stock_data)}

        Determine an outlook for SELLING OPTIONS PREMIUM. The outlook must be Bullish, Bearish, or Neutral.
        
        Your justification must be brief and synthesized from all available data, following these rules:
        - A high "iv_hv_spread_percent" (e.g., > 10) is a strong bullish indicator to sell premium.
        - A high positive "skew_25_delta" (e.g., > 5) is a strong bullish indicator to sell puts, as it signals fear.
        - A high "vix_rank" (e.g., > 50) provides a good environment for selling premium in general.
        - Check for upcoming earnings or dividend dates and mention them if they are soon, as they increase risk.
        
        Your entire response must be a single markdown table row using the exact format:
        | TICKER | $PRICE | Outlook | Justification |
        """
        
        logging.info(f"Synthesizing report for: {stock_data.get('ticker')}")
        response = await llm.ainvoke(single_stock_prompt)
        table_row = response.content.strip().replace("'", "")
        
        # Print each row to the console as it's generated
        print(table_row)
        # Add the generated row to our report list
        report_lines.append(table_row)

    # --- ✅ V2: Write the final report to a file ---
    try:
        report_content = "\n".join(report_lines)
        report_filename = "stock_report.txt"
        with open(report_filename, "w") as f:
            f.write(f"Stock Analysis Report - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("="*50 + "\n\n")
            f.write(report_content)
        logging.info(f"✅ Final report saved to {report_filename}")
    except Exception as e:
        logging.error(f"Failed to write final report file: {e}")

    logging.info("✅ Workflow Finished!")


# --- Main Execution Block ---
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="LLM-Powered Trading Agent")
    parser.add_argument(
        "--tickers", 
        type=str,
        required=True,
        help="The path to a JSON file containing a list of tickers to analyze."
    )
    args = parser.parse_args()
    
    try:
        with open(args.tickers, 'r') as f:
            ticker_list = json.load(f)
        if not isinstance(ticker_list, list):
            raise ValueError("Ticker file must contain a valid JSON list.")
    except Exception as e:
        logging.error(f"Error reading or parsing ticker file: {e}")
        exit(1)
        
    logging.info("Agent starting up...")
    asyncio.run(run_trading_analysis_workflow(tickers=ticker_list))
