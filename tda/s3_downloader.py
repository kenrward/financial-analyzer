import os
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from datetime import datetime, timedelta
from dotenv import load_dotenv
import logging

# --- Setup ---
load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("s3_downloader.log"),
        logging.StreamHandler()
    ]
)

# --- Configuration ---
# Your Polygon.io S3 Credentials from .env file
ACCESS_KEY = os.getenv("POLYGON_ACCESS_KEY")
SECRET_KEY = os.getenv("POLYGON_SECRET_KEY")

# S3 Bucket Details
BUCKET_NAME = "flatfiles"
ENDPOINT_URL = "https://files.polygon.io"

# Local folder to save the data
DESTINATION_ROOT = "/mnt/shared-drive/polygon_data"

# Prefixes for the data types you want to download
# The script will loop through this list.
DATA_PREFIXES = [
    'us_stocks_sip/day_aggs_v1',
    'us_options_opra/day_aggs_v1'
]

# Number of past days to sync data for
DAYS_TO_SYNC = 7

# --- Main Script ---

def sync_polygon_data():
    """
    Constructs file paths for the last N days for specified data prefixes
    and downloads them if they don't already exist locally.
    """
    if not ACCESS_KEY or not SECRET_KEY:
        logging.error("S3 credentials not found. Please check your .env file.")
        return

    # Initialize the S3 client
    s3 = boto3.client(
        's3',
        endpoint_url=ENDPOINT_URL,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        config=Config(signature_version='s3v4'),
    )

    # Loop through each data type prefix (stocks, options, etc.)
    for prefix in DATA_PREFIXES:
        logging.info(f"--- Starting sync for data type: {prefix} ---")

        # Loop through each of the past N days
        for i in range(DAYS_TO_SYNC):
            target_date = datetime.now() - timedelta(days=i)
            year = target_date.strftime('%Y')
            month = target_date.strftime('%m')
            date_str = target_date.strftime('%Y-%m-%d')
            
            # Construct the S3 object key based on Polygon's path structure
            object_key = f"{prefix}/{year}/{month}/{date_str}.csv.gz"

            # Construct the full local path to save the file
            local_filepath = os.path.join(DESTINATION_ROOT, object_key)

            # 1. Skip if the file already exists locally
            if os.path.exists(local_filepath):
                logging.info(f"Skipping {object_key}, file already exists.")
                continue

            # 2. Create the destination directory if it doesn't exist
            os.makedirs(os.path.dirname(local_filepath), exist_ok=True)

            # 3. Attempt to download the specific file
            try:
                logging.info(f"Downloading: {object_key}")
                s3.download_file(BUCKET_NAME, object_key, local_filepath)
                logging.info(f"Successfully downloaded {object_key}")

            except ClientError as e:
                # Handle cases where the file doesn't exist (holiday/weekend) or access is denied
                error_code = e.response.get("Error", {}).get("Code")
                if error_code == '404' or error_code == 'NoSuchKey':
                    logging.warning(f"--> File not found on server: {object_key} (likely a holiday/weekend).")
                elif error_code == '403' or error_code == 'Forbidden':
                    logging.warning(f"--> Access denied for {object_key}. Skipping.")
                else:
                    logging.error(f"--> An unexpected error occurred for {object_key}: {e}.")
    
    logging.info("--- Sync process complete. ---")


if __name__ == "__main__":
    sync_polygon_data()