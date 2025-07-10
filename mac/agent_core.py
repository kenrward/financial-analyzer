# agent_core.py

import asyncio
import json
import logging
from langchain_ollama import ChatOllama
from langchain_core.messages import HumanMessage, ToolMessage

from api_tools import tools
from langgraph.prebuilt import create_react_agent

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
        if kind == "on_agent_finish":
            agent_finish_output = event['data'].get('output')
            if agent_finish_output and agent_finish_output.return_values:
                 raw_data_json_string = agent_finish_output.return_values.get('output', "")

    if not raw_data_json_string:
        logging.error("❗️ Tool execution did not produce a final output string. Cannot proceed to Step 2.")
        return

    logging.info("STEP 1 Complete: Raw data successfully retrieved.")
    logging.debug(f"Full data payload from tool: {raw_data_json_string}")

    # --- STEP 2: Iteratively Synthesize the data ---
    logging.info("STEP 2: Starting iterative synthesis of the report...")
    
    try:
        results_list = json.loads(raw_data_json_string)
    except json.JSONDecodeError as e:
        logging.error(f"❗️ Failed to parse JSON data from Step 1. Error: {e}")
        logging.error(f"--- Data that failed to parse ---:\n{raw_data_json_string}\n---")
        return

    print("\n\n--- FINAL REPORT ---")
    print("| Ticker | Price | Outlook | Justification |")
    print("| :--- | :--- | :--- | :--- |")

    for stock_data in results_list:
        single_stock_prompt = f"""
        You are a financial analyst... Your entire response must be a single markdown table row...
        The data is: {json.dumps(stock_data)}
        """
        
        logging.info(f"Synthesizing report for: {stock_data.get('ticker')}")
        response = await llm.ainvoke(single_stock_prompt)
        table_row = response.content.strip().replace("'", "")
        print(table_row)

    logging.info("✅ Workflow Finished!")


# --- Main Execution Block ---
if __name__ == '__main__':
    logging.info("Agent starting up...")
    logging.info(f"Ollama Model: {OLLAMA_MODEL} at {OLLAMA_BASE_URL}")
    initial_user_query = "Give me a full trading analysis of the top 25 most active stocks."
    asyncio.run(run_trading_analysis_workflow(initial_user_query))