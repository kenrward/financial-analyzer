#!/bin/zsh

# --- Configuration ---
# The root directory of your project
PROJECT_DIR="/Users/kenward/financial-analyzer"
# The directory to save reports in
REPORTS_DIR="$PROJECT_DIR/reports"
# --- End Configuration ---

# Ensure the reports directory exists
mkdir -p "$REPORTS_DIR"

# Define the output file with the current date (YYYYMMDD format)
OUTPUT_FILE="$REPORTS_DIR/$(date +%Y%m%d)-stock-report.txt"

# Run the Python script, sending all output (stdout and stderr) to the file
"$PROJECT_DIR/.venv/bin/python" \
"$PROJECT_DIR/mac/agent_core.py" \
--tickers "$PROJECT_DIR/filtered_optionable_tickers.json" > "$OUTPUT_FILE" 2>&1
