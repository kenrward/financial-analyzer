import json
import asyncio
from langchain_ollama import ChatOllama
# from langchain.agents import AgentExecutor, create_tool_calling_agent # REMOVE THIS LINE
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage # Added ToolMessage
from api_tools import tools # Directly import the 'tools' list

from langgraph.prebuilt import create_react_agent # <--- NEW IMPORT for LangGraph

# --- Configuration ---
OLLAMA_BASE_URL = "http://localhost:11434"
OLLAMA_MODEL = "llama3.1" # <--- CHANGED TO A TOOL-CALLING CAPABLE MODEL

llm = ChatOllama(model=OLLAMA_MODEL, base_url=OLLAMA_BASE_URL, temperature=0.1)

# --- Define the Agent's Tools ---
# The 'tools' list is imported from api_tools.py

# --- Define the Agent's Prompt ---
# create_react_agent typically builds its own prompt internally.
# However, we can still provide a system message if needed, or use a simpler structure.
# For create_react_agent, the input messages are typically handled.
# Let's use a simple system prompt, create_react_agent usually has good defaults.
system_prompt = (
    "You are a highly skilled AI trading assistant named 'TradeSetupFinder'.\n"
    "Your primary goal is to identify potential short-term stock trading setups (bullish or bearish) "
    "by analyzing market data, technical indicators, and news sentiment.\n"
    "You have access to specialized tools to gather real-time financial data.\n"
    "Follow these steps meticulously:\n"
    "1. Start by using `get_most_active_stocks` to identify the current top 5-10 most active stocks. This provides a good starting universe.\n"
    "2. For each promising stock from the active list, get its `historical_data` (at least 60-90 days).\n"
    "3. Then, use `analyze_technical_patterns` with the historical data to identify technical signals (indicators, crossovers, basic patterns).\n"
    "4. If significant technical signals are found, use `get_news_for_ticker` to retrieve recent news for that stock (last 3-7 days).\n"
    "5. Critically analyze the technical signals and news sentiment. Determine if the news sentiment aligns with (confirms) or contradicts the technical pattern.\n"
    "6. If a potential setup (bullish or bearish) is identified and confirmed by news sentiment, present it in a clear, concise, and structured JSON format. \n"
    "   Your output for a setup MUST be a JSON object with the following keys:\n"
    "   `{'ticker': 'STRING', 'type': 'BULLISH' or 'BEARISH', 'technical_signal': 'STRING (e.g., SMA Crossover: Bullish)', 'news_summary': 'STRING (brief summary of relevant news and its sentiment)', 'rationale': 'STRING (explain why this is a setup, connecting TA and news)', 'confidence': 'LOW/MEDIUM/HIGH'}`\n"
    "7. If no strong setups are found for a stock after analysis, state why (e.g., 'No clear setup found due to conflicting signals' or 'Insufficient data').\n"
    "8. Process up to 5-10 stocks from the active list, or until you find a few strong setups. Do not try to analyze all 100 stocks in one go.\n"
    "9. Always be cautious; financial markets are complex. State any uncertainties or risks where appropriate.\n"
    "10. When you have analyzed enough stocks or found enough setups, output your final findings. If no setups are found, state that.\n"
    "Begin by generating a tool call to `get_most_active_stocks`." # Direct the agent to start with this tool call
)

# --- Create the Agent using create_react_agent (from LangGraph) ---
# This automatically handles prompt construction, tool binding, and agent execution flow.
agent_executor = create_react_agent(llm, tools)

# --- Main Agent Execution Function ---
async def run_trading_agent(query: str = "Find potential short-term stock trading setups."):
    print(f"\n--- Running TradeSetupFinder Agent ---\nQuery: {query}\n")

    # create_react_agent expects 'messages' as the primary input.
    # It handles agent_scratchpad and tool calling internally.
    messages = [HumanMessage(content=query)]

    try:
        full_response = ""
        # Stream events from the agent executor
        async for state in agent_executor.astream_events({"messages": messages}, version="v1"):
            kind = state["event"]
            if kind == "on_chain_start":
                print(f"Chain started: {state['name']}")
            elif kind == "on_chain_end":
                print(f"Chain ended: {state['name']}")
            elif kind == "on_llm_start":
                print(f"LLM started: {state['name']}")
            elif kind == "on_llm_end":
                print(f"LLM ended: {state['name']}")
                # print(f"LLM output: {state['data']['chunk'].content}") # For full token output
            elif kind == "on_tool_start":
                print(f"Tool started: {state['name']} with input {state['data'].get('input')}")
            elif kind == "on_tool_end":
                print(f"Tool ended: {state['name']} with output (truncated): {str(state['data'].get('output'))[:200]}...")
            elif kind == "on_agent_action":
                print(f"Agent Action: {state['name']}")
            elif kind == "on_agent_finish":
                # This captures the final output from the LLM when it decides to finish
                final_message = state['data']['output']['messages'][-1]
                print("\n--- Agent Final Response ---")
                if final_message.tool_calls:
                    print(f"Tool Calls made: {final_message.tool_calls}")
                print(f"Content: {final_message.content}")
                full_response = final_message.content
                break # Break after agent finishes its final response

        return full_response

    except Exception as e:
        print(f"\n--- An error occurred during agent execution ---")
        print(f"Error: {e}")
        print("Please check agent logs or container logs for details.")
        return f"Agent execution failed: {e}"

# --- Main Execution Block ---
if __name__ == '__main__':
    print("Starting agent... Ensure Ollama server is running and `0xroyce/plutus:latest` model is available.")
    print(f"Ollama Model: {OLLAMA_MODEL} at {OLLAMA_BASE_URL}")

    # Run the agent with an initial query
    asyncio.run(run_trading_agent(
        "Find potential short-term stock trading setups."
    ))