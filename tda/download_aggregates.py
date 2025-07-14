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
bucket_name = 'flatfiles'

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


    for page in paginator.paginate(Bucket='flatfiles', Prefix=prefix):
        for obj in page['Contents']:
            print(obj['Key'])
            local_filepath = os.path.join(DESTINATION_DIR, f"{obj['Key']}")
            # Skip if the file already exists locally
            if os.path.exists(local_filepath):
                print(f"Skipping {obj['Key']}, already downloaded.")
                continue
            #s3.download_file(bucket_name, obj['Key'], local_filepath)

if __name__ == "__main__":
    download_polygon_data()