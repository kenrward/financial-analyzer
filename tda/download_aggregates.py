import os
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError  # <-- Import ClientError
from datetime import datetime, timedelta
from dotenv import load_dotenv

# --- (Your existing configuration code remains here) ---
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
prefix = 'us_options_opra'  # Example: Change this prefix to match your data need

# S3 Bucket Details
BUCKET_NAME = "flatfiles"
ENDPOINT_URL = "https://files.polygon.io"

# Download settings
DESTINATION_DIR = "/mnt/shared-drive/polygon_data" # Local folder to save files
AGG_TYPE = "minute_aggs_v1" # We want 1-minute aggregates
bucket_name = 'flatfiles'

# --- Main Script ---

def download_polygon_data():
    """
    Downloads polygon data, skipping files that are already downloaded
    or result in a 403 Forbidden error.
    """
    if not ACCESS_KEY or not SECRET_KEY:
        print("Error: S3 credentials not found in .env file.")
        print("Please create a .env file with your POLYGON_ACCESS_KEY and POLYGON_SECRET_KEY.")
        return

    # Create the base destination directory if it doesn't exist
    os.makedirs(DESTINATION_DIR, exist_ok=True)

    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        if 'Contents' not in page:
            continue
        for obj in page['Contents']:
            key = obj['Key']
            local_filepath = os.path.join(DESTINATION_DIR, key)

            # Skip if the file already exists locally
            if os.path.exists(local_filepath):
                # print(f"Skipping {key}, already downloaded.")
                continue

            # Create destination subdirectory if it doesn't exist
            os.makedirs(os.path.dirname(local_filepath), exist_ok=True)

            try:
                print(f"Downloading: {key}")
                s3.download_file(bucket_name, key, local_filepath)

            except ClientError as e:
                # Check if the error is a 403 Forbidden error
                if e.response['Error']['Code'] == '403' or e.response['Error']['Code'] == 'Forbidden':
                    print(f"--> Access denied for {key}. Skipping file.")
                else:
                    # Handle other client errors (e.g., 404 Not Found)
                    print(f"--> An unexpected error occurred for {key}: {e}. Skipping.")
                continue # Move on to the next file

if __name__ == "__main__":
    download_polygon_data()