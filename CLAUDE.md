# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

Financial-analyzer is a sophisticated options trading analysis system that specializes in analyzing stocks for premium selling opportunities. It uses AI/LLM analysis to evaluate stocks based on volatility metrics, technical indicators, and market context to provide recommendations for options traders.

## Architecture

The system follows a **microservices architecture** with three main API services:

1. **TDA (Trade Data API)** - `tda/` - Handles market data collection from Polygon.io
2. **TTA (Technical Analysis API)** - `tta/` - Performs technical analysis on stock data  
3. **TOA (Technical Options API)** - `toa/` - Specialized options analysis including IV vs HV spread

Main orchestration happens in `mac/agent_core.py` which coordinates the analysis workflow and calls the microservices via `mac/api_tools.py`.

## Tech Stack

- **Python 3.9+** with Flask microservices
- **LangChain + Ollama** (llama3.1 model) for local LLM inference on port 11434
- **Polygon.io API** for market data (requires API key)
- **Parquet files** for historical data storage at `/mnt/shared-drive/us_stocks_daily.parquet`
- **AsyncIO + HTTPX** for concurrent API processing with semaphore-based rate limiting

## Key Commands

### Setup and Dependencies
```bash
pip install -r requirements.txt
```

### Running the Application
```bash
# Main execution script
./run_analyzer.sh

# Or manually:
python mac/agent_core.py --tickers filtered_optionable_tickers.json
```

### Starting Microservices
Each service needs to be running independently:
```bash
# Start each in separate terminals
python tda/app.py
python tta/app.py  
python toa/app.py
```

### Health Checks
```bash
curl http://localhost:5000/health  # TDA
curl http://localhost:5001/health  # TTA
curl http://localhost:5002/health  # TOA
```

## Important Files and Directories

- `mac/agent_core.py` - Main orchestration engine
- `mac/api_tools.py` - Core integration layer for microservices
- `mac/screener/Copilot.py` - Stock screening tool
- `filtered_optionable_tickers.json` - Target universe of stocks
- `reports/` - Daily generated analysis reports in markdown format
- `Archive/` - Previous iteration with backtesting and sentiment analysis

## Environment Variables

- `POLYGON_API_KEY` - Required for market data access
- Ensure Ollama is running locally on port 11434

## Data Flow

1. **Screening**: Filter optionable stocks with high liquidity (>1M volume), exclude earnings
2. **Multi-dimensional Analysis**: Price data, technical analysis, options analysis, news sentiment
3. **AI Synthesis**: Local LLM generates structured recommendations for premium selling
4. **Reporting**: Output to console and daily reports in `/reports/`

## Known Issues

- Hard-coded log path in `agent_run.log` may cause read-only filesystem errors
- SSL warnings from urllib3/LibreSSL compatibility  
- Requires external dependencies (Ollama, microservices) to be running

## Development Notes

The system uses async-first architecture with rate limiting (8 concurrent requests), efficient Parquet-based data storage, and focuses specifically on options premium selling opportunities rather than directional trading.