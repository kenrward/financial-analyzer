# agent_core.py

import asyncio
import json
from langchain_ollama import ChatOllama
from langchain_core.messages import HumanMessage, ToolMessage

from api_tools import tools
from langgraph.prebuilt import create_react_agent

# --- Configuration ---
OLLAMA_BASE_URL = "http://localhost:11434"
OLLAMA_MODEL = "command-r-plus:latest" #"llama3.1" 

llm = ChatOllama(model=OLLAMA_MODEL, base_url=OLLAMA_BASE_URL, temperature=0.2)

# --- Agent 1: The Data Retriever ---
data_retrieval_agent = create_react_agent(llm, tools)


# --- The Main Orchestration Function ---
async def run_trading_analysis_workflow(query: str):
    print(f"\n🚀 --- Kicking off Scalable Agent Workflow --- 🚀\nInitial Query: {query}\n")

    # --- STEP 1: Run the Data Retriever agent to get all the raw data at once ---
    print("--- STEP 1: Calling data retrieval agent to execute tools... ---")
    retrieval_inputs = {"messages": [HumanMessage(content=query)]}
    raw_data_json_string = ""

    async for event in data_retrieval_agent.astream_events(retrieval_inputs, version="v1"):
        kind = event["event"]
        if kind == "on_tool_end":
            tool_output = event["data"].get("output")
            # ✅ --- THE FIX --- ✅
            # Ensure we get the string content from the ToolMessage object
            if isinstance(tool_output, ToolMessage):
                raw_data_json_string = tool_output.content
            else:
                raw_data_json_string = str(tool_output)


    if not raw_data_json_string:
        print("\n--- ❗️ Tool execution failed. Could not retrieve data. ---")
        return

    print("\n\n--- STEP 1 Complete: Raw data successfully retrieved. ---")

    # --- STEP 2: Iteratively Synthesize the data, one stock at a time ---
    print("\n--- STEP 2: Starting iterative synthesis of the report... ---")
    
    # Parse the full JSON string into a Python list
    results_list = json.loads(raw_data_json_string)
    
    # Print the markdown table header first
    print("\n\n| Ticker | Price | Outlook | Justification |")
    print("| :--- | :--- | :--- | :--- |")

    # Loop through each stock's data
    for stock_data in results_list:
        # A focused prompt for analyzing just ONE stock
        single_stock_prompt = f"""
        You are a financial analyst. Your task is to analyze the data for a single stock and provide a one-line summary for a markdown table.
        The data is: {json.dumps(stock_data)}

        Determine if the outlook is Bullish, Bearish, or Neutral based on the technicals and news.
        
        Your entire response must be a single markdown table row using the format:
        | TICKER | $PRICE | Outlook | Justification |
        """
        
        # Use invoke for a single, non-streamed response
        response = await llm.ainvoke(single_stock_prompt)
        # Clean up the response and print the table row
        table_row = response.content.strip().replace("'", "")
        print(table_row)

    print("\n\n✅ --- STEP 2 Complete: Workflow Finished! --- ✅")


# --- Main Execution Block ---
if __name__ == '__main__':
    print("Starting agent...")
    print(f"Ollama Model: {OLLAMA_MODEL} at {OLLAMA_BASE_URL}")

    # The user's query determines how many stocks the tool will fetch.
    initial_user_query = "Give me a full trading analysis of the top 25 most active stocks."

    asyncio.run(run_trading_analysis_workflow(initial_user_query))