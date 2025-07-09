# agent_core.py

import asyncio
import json
from langchain_ollama import ChatOllama
from langchain_core.messages import HumanMessage

from api_tools import tools
from langgraph.prebuilt import create_react_agent

# --- Configuration ---
OLLAMA_BASE_URL = "http://localhost:11434"
OLLAMA_MODEL = "llama3.1" 

llm = ChatOllama(model=OLLAMA_MODEL, base_url=OLLAMA_BASE_URL, temperature=0.2)

# --- Agent 1: The Data Retriever ---
# This agent's only job is to call our single "super-tool"
data_retrieval_agent = create_react_agent(llm, tools)


# --- The Main Orchestration Function ---
async def run_trading_analysis_workflow(query: str):
    print(f"\n🚀 --- Kicking off Two-Step Agent Workflow --- 🚀\nInitial Query: {query}\n")

    # --- STEP 1: Run the Data Retriever agent to get the raw data ---
    print("--- STEP 1: Calling data retrieval agent to execute tools... ---")
    retrieval_inputs = {"messages": [HumanMessage(content=query)]}
    raw_data_json = ""

    async for event in data_retrieval_agent.astream_events(retrieval_inputs, version="v1"):
        kind = event["event"]
        if kind == "on_tool_end":
            # Capture the full, raw JSON output from our "super-tool"
            raw_data_json = event["data"].get("output")

    if not raw_data_json:
        print("\n--- ❗️ Tool execution failed. Could not retrieve data. ---")
        return

    print("\n\n--- STEP 1 Complete: Raw data successfully retrieved. ---")

    # --- STEP 2: Run the Data Synthesizer LLM Call ---
    print("\n--- STEP 2: Calling synthesis LLM to generate the final report... ---")
    
    # The dedicated prompt for the synthesis step
    synthesis_prompt = f"""
    You are a senior financial analyst. Your task is to synthesize the following JSON data into a summary report.
    Do not explain the JSON. Do not show the raw data.
    
    For each stock in the data, determine if the outlook is Bullish, Bearish, or Neutral. 
    Your justification should be brief and based on the provided technical indicators and news.

    Present the final report as a markdown table with columns: 
    Ticker, Price, Outlook, and Justification.

    Here is the data:
    {raw_data_json}
    """

    # Make a direct, tool-less call to the LLM for the final analysis
    final_report = ""
    async for chunk in llm.astream(synthesis_prompt):
        print(chunk.content, end="", flush=True)
        final_report += chunk.content
    
    print("\n\n✅ --- STEP 2 Complete: Workflow Finished! --- ✅")
    return final_report


# --- Main Execution Block ---
if __name__ == '__main__':
    print("Starting agent...")
    print(f"Ollama Model: {OLLAMA_MODEL} at {OLLAMA_BASE_URL}")

    # A simple query for the first agent. The complexity is in the second prompt.
    initial_user_query = "Give me a full trading analysis of the top 25 most active stocks."

    asyncio.run(run_trading_analysis_workflow(initial_user_query))