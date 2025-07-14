import boto3
from dotenv import load_dotenv
import os
from botocore.config import Config

# Initialize a session using your credentials
session = boto3.Session(
  aws_access_key_id= os.getenv("POLYGON_ACCESS_KEY"),
  aws_secret_access_key=os.getenv("POLYGON_SECRET_KEY"),
)
print(os.getenv("POLYGON_ACCESS_KEY"))
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

# List objects using the selected prefix
for page in paginator.paginate(Bucket='flatfiles', Prefix=prefix):
  for obj in page['Contents']:
    print(obj['Key'])

# Copy example
# Specify the bucket name
bucket_name = 'flatfiles'

# Specify the S3 object key name
object_key = 'us_stocks_sip/trades_v1/2025/03/2024-03-07.csv.gz'

# Specify the local file name and path to save the downloaded file
# This splits the object_key string by '/' and takes the last segment as the file name
local_file_name = object_key.split('/')[-1]

# This constructs the full local file path
local_file_path = './' + local_file_name

# Download the file
s3.download_file(bucket_name, object_key, local_file_path)
