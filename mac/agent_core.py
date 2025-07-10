# agent_core.py

import asyncio
import json
import logging # Import the logging library
from langchain_ollama import ChatOllama
from langchain_core.messages import HumanMessage, ToolMessage

from api_tools import tools
from langgraph.prebuilt import create_react_agent

# --- ⚙️ Set up Logging ---
# This will create a file named 'agent_run.log' in the same directory.
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("agent_run.log"), # Log to a file
        logging.StreamHandler() # Also log to the console
    ]
)

# --- Configuration ---
OLLAMA_BASE_URL = "http://localhost:11434"
OLLAMA_MODEL = "llama3.1" 
llm = ChatOllama(model=OLLAMA_MODEL, base_url=OLLAMA_BASE_URL, temperature=0.2)

# --- Agent 1: The Data Retriever ---
data_retrieval_agent = create_react_agent(llm, tools)

# --- The Main Orchestration Function ---
async def run_trading_analysis_workflow(query: str):
    logging.info(f"🚀 Kicking off Scalable Agent Workflow with Query: {query}")

    # --- STEP 1: Run the Data Retriever agent ---
    logging.info("STEP 1: Calling data retrieval agent to execute tools...")
    retrieval_inputs = {"messages": [HumanMessage(content=query)]}
    raw_data_json_string = ""

    async for event in data_retrieval_agent.astream_events(retrieval_inputs, version="v1"):
        kind = event["event"]
        if kind == "on_tool_end":
            tool_output = event["data"].get("output")
            if isinstance(tool_output, ToolMessage):
                raw_data_json_string = tool_output.content
            else:
                raw_data_json_string = str(tool_output)

    if not raw_data_json_string:
        logging.error("❗️ Tool execution failed. Could not retrieve data.")
        return

    logging.info("STEP 1 Complete: Raw data successfully retrieved.")
    # Log the full raw data to the file for debugging the price issue
    logging.debug(f"Full data payload from tool: {raw_data_json_string}")


    # --- STEP 2: Iteratively Synthesize the data ---
    logging.info("STEP 2: Starting iterative synthesis of the report...")
    results_list = json.loads(raw_data_json_string)
    
    final_report_rows = []
    for stock_data in results_list:
        single_stock_prompt = f"""
        You are a financial analyst... (prompt remains the same)
        The data is: {json.dumps(stock_data)}
        ...
        """
        
        logging.info(f"Synthesizing report for: {stock_data.get('ticker')}")
        response = await llm.ainvoke(single_stock_prompt)
        table_row = response.content.strip().replace("'", "")
        final_report_rows.append(table_row)

    # --- Print the final formatted table ---
    print("\n\n--- FINAL REPORT ---")
    print("| Ticker | Price | Outlook | Justification |")
    print("| :--- | :--- | :--- | :--- |")
    for row in final_report_rows:
        print(row)

    logging.info("✅ Workflow Finished!")

# --- Main Execution Block ---
if __name__ == '__main__':
    logging.info("Agent starting up...")
    initial_user_query = "Give me a full trading analysis of the top 25 most active stocks."
    asyncio.run(run_trading_analysis_workflow(initial_user_query))