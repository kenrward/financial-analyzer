import json
import asyncio
from langchain_ollama import ChatOllama
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnablePassthrough, RunnableLambda
from langchain.agents import AgentExecutor, tools_to_json_callable
from langchain.agents.output_parsers import OpenAIToolsAgentOutputParser
from langchain.agents.format_scratchpad import format_to_openai_tool_messages
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage

import api_tools # Import our custom API tools

# --- Configuration ---
OLLAMA_BASE_URL = "http://localhost:11434"
OLLAMA_MODEL = "0xroyce/plutus:latest"

# Instantiate the LLM
# Note: temperature=0.1 for more factual responses, adjust if you want more creativity
llm = ChatOllama(model=OLLAMA_MODEL, base_url=OLLAMA_BASE_URL, temperature=0.1)

# --- Define the Agent's Tools ---
tools = [
    api_tools.get_most_active_stocks,
    api_tools.get_historical_data,
    api_tools.get_news_for_ticker,
    api_tools.analyze_technical_patterns
]

# LangChain needs to know about the tools. We'll bind them to the LLM.
# tools_to_json_callable converts our @tool decorated functions into the format
# the LLM expects for tool calling.
llm_with_tools = llm.bind_tools(tools_to_json_callable(tools))


# --- Define the Agent's Prompt ---
# We now explicitly list the tools in the system message.
# The agent_scratchpad is managed by LangChain for tool-calling agents.
prompt = ChatPromptTemplate.from_messages(
    [
        SystemMessage(
            content=(
                "You are a highly skilled AI trading assistant named 'TradeSetupFinder'.\n"
                "Your primary goal is to identify potential short-term stock trading setups (bullish or bearish) "
                "by analyzing market data, technical indicators, and news sentiment.\n"
                "You have access to specialized tools to gather financial data. Use them strategically.\n\n"
                "Follow these steps meticulously:\n"
                "1. Start by using `get_most_active_stocks` to identify the current top 100 most active stocks. This provides a good starting universe.\n"
                "2. For each promising stock from the active list, get its `historical_data` (at least 60-90 days).\n"
                "3. Then, use `analyze_technical_patterns` with the historical data to identify technical signals (indicators, crossovers, basic patterns).\n"
                "4. If significant technical signals are found, use `get_news_for_ticker` to retrieve recent news for that stock (last 3-7 days).\n"
                "5. Critically analyze the technical signals and news sentiment. Determine if the news sentiment aligns with (confirms) or contradicts the technical pattern.\n"
                "6. If a potential setup (bullish or bearish) is identified and confirmed by news sentiment, present it in a clear, concise, and structured format. \n"
                "   Your output for a setup MUST be a JSON object with the following keys:\n"
                "   `{'ticker': 'STRING', 'type': 'BULLISH' or 'BEARISH', 'technical_signal': 'STRING (e.g., SMA Crossover: Bullish)', 'news_summary': 'STRING (brief summary of relevant news and its sentiment)', 'rationale': 'STRING (explain why this is a setup, connecting TA and news)', 'confidence': 'LOW/MEDIUM/HIGH'}`\n"
                "7. If no strong setups are found for a stock after analysis, state why (e.g., 'No clear setup found due to conflicting signals' or 'Insufficient data').\n"
                "8. Process up to 5-10 stocks from the active list, or until you find a few strong setups. Do not try to analyze all 100 stocks in one go.\n"
                "9. Always be cautious; financial markets are complex. State any uncertainties or risks where appropriate.\n"
                "10. When you have analyzed enough stocks or found enough setups, output your final findings. If no setups are found, state that.\n\n"
                # Describe available tools explicitly if the LLM isn't inferring well
                # This often helps in custom agent implementations
                f"Available tools:\n{json.dumps([t.name for t in tools], indent=2)}\n" # List tool names for the LLM
            )
        ),
        # chat_history will contain HumanMessage/AIMessage for previous turns
        MessagesPlaceholder(variable_name="chat_history"),
        ("human", "{input}"),
        # This is where the agent's internal thoughts and tool outputs go
        MessagesPlaceholder(variable_name="agent_scratchpad"),
    ]
)

# --- Construct the Agent Runnable ---
# This chain handles the agent's thought process:
# 1. Format intermediate steps (tool calls and their outputs) into messages
# 2. Pass everything to the LLM (which has tools bound)
# 3. Parse the LLM's output to extract tool calls or a final answer
agent_runnable = RunnablePassthrough.assign(
    agent_scratchpad=lambda x: format_to_openai_tool_messages(x["intermediate_steps"])
) | prompt | llm_with_tools | OpenAIToolsAgentOutputParser()

# Create the Agent Executor
agent_executor = AgentExecutor(
    agent=agent_runnable, # Use our new runnable
    tools=tools,
    verbose=True, # Keep verbose for debugging
    handle_parsing_errors=True,
    max_iterations=25, # Increased iterations slightly as it might take more steps
    early_stopping_method="generate",
)

# --- Main Agent Execution Function ---
async def run_trading_agent(query: str):
    print(f"\n--- Running TradeSetupFinder Agent ---\nQuery: {query}\n")

    chat_history = [] # For a fresh run, history is empty

    try:
        # We no longer need the "ticker": "DUMMY" workaround here.
        # The agent structure explicitly defines inputs as 'input', 'chat_history', and 'intermediate_steps'
        response_stream = agent_executor.stream(
            {"input": query, "chat_history": chat_history, "intermediate_steps": []},
        )

        final_output_parts = []
        for s in response_stream:
            if isinstance(s, dict) and "output" in s:
                final_output_parts.append(s["output"])
            # The verbose=True will print tool calls and observations via agent_executor
            # print(s) # Uncomment for extremely detailed stream events

        full_response = "".join(final_output_parts)
        print("\n--- Agent Final Response ---")
        print(full_response)
        return full_response

    except Exception as e:
        print(f"\n--- An error occurred during agent execution ---")
        print(f"Error: {e}")
        print("Please check agent logs (set verbose=True) or container logs for details.")
        return f"Agent execution failed: {e}"

# --- Main Execution Block ---
if __name__ == '__main__':
    print("Starting agent... Ensure Ollama server is running and `0xroyce/plutus:latest` model is available.")
    print(f"Ollama Model: {OLLAMA_MODEL} at {OLLAMA_BASE_URL}")

    asyncio.run(run_trading_agent(
        "Find potential short-term stock trading setups. Start by looking at the top 5 most active stocks, "
        "then analyze technicals and news for each. Provide structured JSON output for any clear setups. "
        "Prioritize stocks with high volume and clear directional momentum."
    ))