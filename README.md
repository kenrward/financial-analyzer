# LLM-Powered Trading Agent: Version 2.0 Recap

The system is a sophisticated, multi-service application designed to find and analyze options trading opportunities. It leverages a local LLM for qualitative analysis, supported by a robust backend architecture for data processing and quantitative calculations.

## Architecture Summary:

**Orchestration (Mac mini):**

* The main `agent_core.py` script runs on your Mac, initiating the analysis and synthesizing the final report using a local Ollama LLM.

**Data Pipeline (Proxmox):**

* An S3 downloader (`s3_downloader.py`) syncs raw daily flat files from Polygon.io to a shared drive.

* A data processor (`downloader.py`) reads these raw files and builds a master historical database in the efficient Parquet format.

**Backend Microservices (Proxmox LXC Containers):**

* **Data API (`data_api.py`):** Serves live data (news, options chains) and reference data (earnings/dividends from Yahoo Finance).

* **Technical Analysis API (`ta_api.py`):** Reads from the local Parquet database to perform technical calculations, including Historical Volatility (HV) and VIX analysis.

* **Options Analysis API (`options_api.py`):** The core V2 engine that calculates IV/HV spread and volatility skew.

## Workflow:

The `agent_core.py` script takes a list of tickers from a file, calls the `api_tools.py` "super-tool" to orchestrate calls to all backend services, and then uses the LLM to analyze the aggregated data and generate a final report.
