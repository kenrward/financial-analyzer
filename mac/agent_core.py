# agent_core_corrected.py

import asyncio
import json
from langchain_ollama import ChatOllama
from langchain_core.messages import HumanMessage
from api_tools import tools  # Directly import the 'tools' list

# Using the prebuilt ReAct agent from LangGraph
from langgraph.prebuilt import create_react_agent

# --- Configuration ---
OLLAMA_BASE_URL = "http://localhost:11434"
# Using the model you confirmed supports tool-calling
OLLAMA_MODEL = "llama3.1" 

# Initialize the LLM
# A slightly higher temperature can sometimes help with reasoning chains
llm = ChatOllama(model=OLLAMA_MODEL, base_url=OLLAMA_BASE_URL, temperature=0.2)

# --- Create the Agent using LangGraph's prebuilt function ---
# This single line creates the entire agent graph. It automatically
# handles tool binding, prompting, and the ReAct loop logic.
agent_executor = create_react_agent(llm, tools)

# --- Main Agent Execution Function ---
async def run_trading_agent(query: str):
    """
    Runs the trading agent, streams the output, and prints the final result.
    """
    print(f"\n🚀 --- Running Trading Agent --- 🚀\nQuery: {query}\n")

    # The input to the agent is a dictionary with a "messages" key.
    inputs = {"messages": [HumanMessage(content=query)]}
    
    final_response = ""

    try:
        # Use astream_events to get a detailed log of the agent's actions.
        # This is great for debugging and seeing the agent's "thoughts".
        async for event in agent_executor.astream_events(inputs, version="v1"):
            kind = event["event"]
            
            # Print events from the agent's main components
            if kind == "on_chat_model_stream":
                # Stream the LLM's thoughts as they happen
                content = event["data"]["chunk"].content
                if content:
                    print(content, end="", flush=True)
            
            elif kind == "on_tool_start":
                # Show which tool is being called with what input
                print("\n\n🛠️  Calling Tool:", event["name"])
                print(f"   - Tool Input: {event['data'].get('input')}")

            elif kind == "on_tool_end":
                # Show the result from the tool call
                print(f"   - Tool Output (truncated): {str(event['data'].get('output'))[:250]}...")

            elif kind == "on_agent_finish":
                # The agent has finished its work
                print("\n\n✅ --- Agent Finished --- ✅")
                final_response = event["data"]["output"]["messages"][-1].content
                print(f"\nFinal Answer:\n{final_response}")

        if not final_response:
             print("\n⚠️ Agent did not produce a final answer.")

    except Exception as e:
        print(f"\n--- ❗️ An error occurred during agent execution ---")
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

    return final_response

# --- Main Execution Block ---
if __name__ == '__main__':
    print("Starting agent...")
    print(f"Ollama Model: {OLLAMA_MODEL} at {OLLAMA_BASE_URL}")

    # ✅ UPDATED: A simpler, more direct query for the agent
    initial_query = "Find the top 5 most active stocks. Then, for each of those stocks, get its technical analysis and recent news. Finally, synthesize all of this information to provide a bullish, bearish, or neutral outlook for each stock."

    asyncio.run(run_trading_agent(initial_query))