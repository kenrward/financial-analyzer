import os
import boto3
from botocore.exceptions import ClientError
from datetime import datetime, timedelta
from dotenv import load_dotenv

# --- Configuration ---
# Load environment variables from .env file
load_dotenv()

# Your Polygon.io S3 Credentials
ACCESS_KEY = os.getenv("POLYGON_ACCESS_KEY")
SECRET_KEY = os.getenv("POLYGON_SECRET_KEY")

# S3 Bucket Details
BUCKET_NAME = "flatfiles"
ENDPOINT_URL = "https://files.polygon.io"

# Download settings
DESTINATION_DIR = "/mnt/shared-drive/polygon_data" # Local folder to save files
DATA_TYPE = "us_stocks_sip" # Data type to download
AGG_TYPE = "minute_aggs" # We want 1-minute aggregates

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

    # Initialize S3 client
    s3_client = boto3.client(
        's3',
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        endpoint_url=ENDPOINT_URL,
        region_name="us-east-1" # This can typically be a default value
    )
    
    print("Successfully connected to Polygon.io S3 storage.")
    
    # Calculate date range (last 180 days)
    end_date = datetime.now() - timedelta(days=1)
    start_date = end_date - timedelta(days=180)

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

        try:
            # Download the file
            s3_client.download_file(BUCKET_NAME, s3_key, local_filepath)
            print(f"-> Successfully downloaded to {local_filepath}")
        
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                # Files for weekends or market holidays won't exist
                print(f"-> File not found. Likely a weekend or holiday.")
            else:
                print(f"-> An error occurred: {e}")

        # Move to the next day
        current_date += timedelta(days=1)

if __name__ == "__main__":
    download_polygon_data()