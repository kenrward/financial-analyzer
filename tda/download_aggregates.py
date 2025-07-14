import os
import boto3
from botocore.config import Config
from datetime import datetime, timedelta
from dotenv import load_dotenv

# --- Configuration ---
# Load environment variables from .env file
load_dotenv()

# Your Polygon.io S3 Credentials
ACCESS_KEY = os.getenv("POLYGON_ACCESS_KEY")
SECRET_KEY = os.getenv("POLYGON_SECRET_KEY")

session = boto3.Session(
  aws_access_key_id=ACCESS_KEY ,
  aws_secret_access_key=SECRET_KEY ,
)

# Create a client with your session and specify the endpoint
s3 = session.client(
  's3',
  endpoint_url='https://files.polygon.io',
  config=Config(signature_version='s3v4'),
)

# List Example
# Initialize a paginator for listing objects
paginator = s3.get_paginator('list_objects_v2')

# Choose the appropriate prefix depending on the data you need:
# - 'global_crypto' for global cryptocurrency data
# - 'global_forex' for global forex data
# - 'us_indices' for US indices data
# - 'us_options_opra' for US options (OPRA) data
# - 'us_stocks_sip' for US stocks (SIP) data
prefix = 'us_stocks_sip'  # Example: Change this prefix to match your data need

# S3 Bucket Details
BUCKET_NAME = "flatfiles"
ENDPOINT_URL = "https://files.polygon.io"

# Download settings
DESTINATION_DIR = "/mnt/shared-drive/polygon_data" # Local folder to save files
DATA_TYPE = "us_stocks_sip" # Data type to download
AGG_TYPE = "minute_aggs_v1" # We want 1-minute aggregates

# --- Main Script ---

def download_polygon_data():
    """
    Downloads the last 6 months of 1-minute stock aggregates from Polygon.io.
    """
    if not ACCESS_KEY or not SECRET_KEY:
        print("Error: S3 credentials not found in .env file.")
        print("Please create a .env file with your POLYGON_ACCESS_KEY and POLYGON_SECRET_KEY.")
        return

    # Create the destination directory if it doesn't exist
    os.makedirs(DESTINATION_DIR, exist_ok=True)


    # Calculate date range (last 180 days)
    end_date = datetime.now() - timedelta(days=1)
    start_date = end_date - timedelta(days=2)

    # Loop through each day in the date range
    current_date = start_date
    while current_date <= end_date:
        # Format the date parts for the S3 key
        year = current_date.strftime('%Y')
        month = current_date.strftime('%m')
        day = current_date.strftime('%d')
        date_str = current_date.strftime('%Y%m%d')

        # Construct the S3 object key (the file path in the bucket)
        # e.g., /us_stocks_sip/minute_aggs/2024/01/20240125.csv.gz
        s3_key = f"{DATA_TYPE}/{AGG_TYPE}/{year}/{month}/{date_str}.csv.gz"
        local_filepath = os.path.join(DESTINATION_DIR, f"{date_str}.csv.gz")

        # Skip if the file already exists locally
        if os.path.exists(local_filepath):
            print(f"Skipping {s3_key}, already downloaded.")
            current_date += timedelta(days=1)
            continue
            
        print(f"Attempting to download: {s3_key}")

        object_key = 'us_stocks_sip/minute_aggs_v1/2025/07/2025-07-10.csv.gz'
        s3.download_file(BUCKET_NAME, object_key, local_filepath)
        print(f"-> Successfully downloaded to {local_filepath}")
        
        # Move to the next day
        current_date += timedelta(days=1)

if __name__ == "__main__":
    download_polygon_data()