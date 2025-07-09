import json
import asyncio
from langchain_ollama import ChatOllama
from langchain_core.prompts import PromptTemplate # <--- CHANGED: Use PromptTemplate
from langchain_core.messages import HumanMessage, AIMessage, ToolMessage # ToolMessage for parsing observations
from api_tools import tools # Directly import the 'tools' list

from langgraph.prebuilt import create_react_agent # For LangGraph's ReAct agent
from langchain.agents.output_parsers.tools import parse_tool_calls # For parsing tool outputs

# --- Configuration ---
OLLAMA_BASE_URL = "http://localhost:11434"
OLLAMA_MODEL = "llama3.1" # Your tool-calling capable model

llm = ChatOllama(model=OLLAMA_MODEL, base_url=OLLAMA_BASE_URL, temperature=0.1)

# --- Define the Agent's Tools ---
# The 'tools' list is directly imported from api_tools.py

# --- Define the Agent's Prompt for ReAct ---
# This template is directly from LangChain's ReAct examples,
# tailored to our goal and tools.
template = """Answer the following questions as best you can. You have access to the following tools:
{tools}

Use the following format:

Question: the input question you must answer
Thought: you should always think about what to do
Action: the action to take, should be one of [{tool_names}]
Action Input: the input to the action
Observation: the result of the action
... (this Thought/Action/Action Input/Observation can repeat N times)
Thought: I now know the final answer
Final Answer: the final answer to the original input question

Begin!

Question: {input}
Thought:{agent_scratchpad}
"""

# Create the PromptTemplate from the string.
# It dynamically inserts tool descriptions/names using render_text_description
# and expects 'input' and 'agent_scratchpad' to be passed.
prompt = PromptTemplate.from_template(template)

# --- Create the Agent using create_react_agent (from LangGraph) ---
# create_react_agent automatically handles tool binding and internal logic.
agent_executor = create_react_agent(
    llm,
    tools,
    prompt # <--- NOW PASSING THE CORRECT PROMPT TEMPLATE
)

# --- Main Agent Execution Function ---
async def run_trading_agent(query: str = "Find potential short-term stock trading setups for the top 5 most active stocks. Analyze their technicals and recent news to confirm."):
    print(f"\n--- Running TradeSetupFinder Agent ---\nQuery: {query}\n")

    # For create_react_agent, the initial input is typically just the human message.
    # The prompt template itself handles inserting tools, tool_names, and agent_scratchpad.
    # We also need to manage the messages list for input to astream_events.
    messages = [HumanMessage(content=query)]

    # --- Internal Agent State Tracking (for ReAct loop) ---
    # Initialize agent scratchpad (for Thought/Action/Observation history)
    agent_scratchpad = "" 

    try:
        final_output_content = ""
        # Stream all events for comprehensive debugging
        async for state in agent_executor.astream_events({"input": query}, version="v1"): # Pass input directly to match prompt variable
            kind = state["event"]
            print(f"\n--- Event: {kind} ({state['name']}) ---")

            if "data" in state:
                if "input" in state["data"]:
                    # This shows inputs to tools or chains
                    print(f"  Input: {str(state['data']['input'])[:500]}...")
                if "output" in state["data"]:
                    output_data = state['data']['output']
                    # Check if output is a list of messages (from LLM) or direct tool output
                    if isinstance(output_data, dict) and 'messages' in output_data:
                        # This is typically the LLM's response or state updates
                        for msg in output_data['messages']:
                            print(f"  Message Type: {type(msg).__name__}")
                            if hasattr(msg, 'tool_calls') and msg.tool_calls:
                                print(f"  Tool Calls: {msg.tool_calls}")
                            if hasattr(msg, 'content'):
                                print(f"  Content: {str(msg.content)[:500]}...")
                                # Update scratchpad with new thought/action/tool call
                                if hasattr(msg, 'tool_calls') and msg.tool_calls:
                                    agent_scratchpad += f"Thought: {msg.content.strip()}\nAction: {msg.tool_calls[0].name}\nAction Input: {msg.tool_calls[0].args}\n"
                                else:
                                    agent_scratchpad += f"Thought: {msg.content.strip()}\n" # Add content to scratchpad

                    else:
                        # This is typically tool output (Observation)
                        print(f"  Output: {str(output_data)[:500]}...")
                        # Update scratchpad with tool observation
                        agent_scratchpad += f"Observation: {str(output_data).strip()}\n" # Add observation to scratchpad

            # Capture the final AI message content when the agent finishes
            if kind == "on_agent_finish":
                final_message_obj = state['data']['output'] # Final output is usually AgentFinish
                if hasattr(final_message_obj, 'return_values') and 'output' in final_message_obj.return_values:
                    final_output_content = final_message_obj.return_values['output']
                    print("\n*** AGENT COMPLETED ITS TASK ***")
                    print(f"Final Content: {final_output_content}")
                    break # Break loop after agent finishes its final response


        if not final_output_content:
            print("\n--- Agent did not provide a final content response. ---")

        return final_output_content

    except Exception as e:
        print(f"\n--- An error occurred during agent execution ---")
        print(f"Error: {e}")
        import traceback
        traceback.print_exc() # Print full traceback for deeper debugging
        print("Please check agent logs or container logs for details.")
        return f"Agent execution failed: {e}"

# --- Main Execution Block ---
if __name__ == '__main__':
    print("Starting agent... Ensure Ollama server is running and `llama3:8b-instruct-q4_K_M` model is available.")
    print(f"Ollama Model: {OLLAMA_MODEL} at {OLLAMA_BASE_URL}")

    asyncio.run(run_trading_agent(
        "Find potential short-term stock trading setups for the top 5 most active stocks. Analyze their technicals and recent news to confirm."
    ))