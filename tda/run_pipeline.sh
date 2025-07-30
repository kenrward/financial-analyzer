#!/bin/bash

# Navigate to the project directory
cd /home/kewar/financial-analyzer || exit

# Define log file and email settings
LOG_FILE="/home/kewar/financial-analyzer/pipeline_run.log"
EMAIL_RECIPIENT="your_email@example.com" # <-- IMPORTANT: Change this to your email

# Function to send an error email
send_error_email() {
  local subject="$1"
  local body="$2"
  echo "$body" | mail -s "$subject" "$EMAIL_RECIPIENT"
}

# --- Start of Pipeline ---
echo "==============================================" >> $LOG_FILE
echo "Pipeline started at $(date)" >> $LOG_FILE

# Step 1: Run the S3 Downloader to get raw flat files
echo "Running S3 Downloader..." >> $LOG_FILE
python3 /home/kewar/financial-analyzer/tda/s3_downloader.py >> $LOG_FILE 2>&1

# Check the exit code of the S3 downloader
if [ $? -ne 0 ]; then
  echo "S3 Downloader FAILED at $(date)" >> $LOG_FILE
  send_error_email "Trading Agent Alert: S3 Downloader FAILED" "The s3_downloader.py script failed. Please check the log file: $LOG_FILE"
  exit 1
fi

# Step 2: Run the Parquet Processor to build the database
echo "Running Parquet Processor..." >> $LOG_FILE
python3 /home/kewar/financial-analyzer/tda/downloader.py >> $LOG_FILE 2>&1

# Check the exit code of the Parquet processor
if [ $? -ne 0 ]; then
  echo "Parquet Processor FAILED at $(date)" >> $LOG_FILE
  send_error_email "Trading Agent Alert: Parquet Processor FAILED" "The downloader.py script failed. Please check the log file: $LOG_FILE"
  exit 1
fi

echo "Pipeline finished successfully at $(date)" >> $LOG_FILE
echo "==============================================" >> $LOG_FILE
