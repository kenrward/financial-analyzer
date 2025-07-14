import boto3
from dotenv import load_dotenv
import os
from botocore.exceptions import ClientError

# Load environment variables from .env file
load_dotenv()

# --- Configuration ---
POLYGON_ACCESS_KEY = os.getenv("POLYGON_ACCESS_KEY")
POLYGON_SECRET_KEY = os.getenv("POLYGON_SECRET_KEY")

if not POLYGON_ACCESS_KEY:
    print("API Key not found. Ensure .env file is correct.")
    exit()

# Create a client with your session and specify the endpoint
s3 = boto3.client(
    's3',
    aws_access_key_id=POLYGON_ACCESS_KEY,
    aws_secret_access_key=POLYGON_SECRET_KEY,
    endpoint_url='https://files.polygon.io',
)

# --- Download Test ---
# 1. Specify the bucket name
bucket_name = 'flatfiles'

# 2. Specify the correct S3 object key for a RECENT MINUTE AGGREGATE file
#    Using a file from last Friday, as today's is not yet available.
object_key = 'us_stocks_sip/minute_aggs/2025/07/20250711.csv.gz'

# 3. Specify the local file path to save the download
local_file_path = './minute_aggs_20250711.csv.gz'

print(f"Attempting to download: {object_key}")

try:
    # 4. Download the file
    s3.download_file(bucket_name, object_key, local_file_path)
    print(f"✅ Success! File downloaded to {local_file_path}")

except ClientError as e:
    if e.response['Error']['Code'] == '403':
        print("❌ Error: 403 Forbidden.")
        print("This confirms your plan does not allow downloading this file type.")
        print("Please contact Polygon.io support to clarify your flat-file permissions.")
    else:
        print(f"An unexpected error occurred: {e}")