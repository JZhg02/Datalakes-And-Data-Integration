import os
import yaml
import requests
import time
import io
import boto3
from datetime import datetime, timedelta

# Load YAML configuration
with open("config/config.yaml", "r") as file:
    config = yaml.safe_load(file)

# Load pollutants
with open("config/pollutants.yaml", "r") as file:
    pollutants = yaml.safe_load(file)

# Get Geodair API configuration parameters
BASE_URL = config["data_source"]["base_url"]
LAST_N_DAYS = config["data_source"]["last_n_days"]  # Number of days to fetch

# Geodair API Key 
API_KEY = os.getenv("GEODAIR_API_KEY")
if not API_KEY:
    raise ValueError("GEODAIR_API_KEY is not set in the environment.")

# S3 configuration 
S3_ENDPOINT_URL = config["s3"]["endpoint_url"]
BUCKET_NAME = config["s3"]["bucket_name"]

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY:
    raise ValueError("AWS_ACCESS_KEY_ID or AWS_SECRET_ACCESS_KEY is not set in the environment.")

# Connect to S3 (LocalStack)
s3 = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT_URL,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)

# Check if bucket exists, create if not
buckets = s3.list_buckets().get("Buckets", [])
bucket_names = [bucket["Name"] for bucket in buckets]

if BUCKET_NAME in bucket_names:
    print(f"Bucket '{BUCKET_NAME}' already exists.")
else:
    s3.create_bucket(Bucket=BUCKET_NAME)
    print(f"Bucket '{BUCKET_NAME}' created successfully!")

# Calculate dates for the last N days
today = datetime.today()
dates = [(today - timedelta(days=i + 1)).strftime("%Y-%m-%d") for i in range(LAST_N_DAYS)]

# Process each date and each pollutant from the YAML file
for formatted_date in dates:
    print(f"\nProcessing data for date: {formatted_date}")

    for pollutant in pollutants:
        pollutant_code = pollutant["code"]
        pollutant_short_name = pollutant["short_name"]
        print(f"\nProcessing pollutant: {pollutant_short_name} (Code: {pollutant_code})")

        # 1. Request generation of the statistics file for the given date and pollutant
        export_url = f"{BASE_URL}/MoyH/export?date={formatted_date}&polluant={pollutant_code}"
        print(f"Requesting file generation via: {export_url}")
        gen_response = requests.get(export_url, headers={"apikey": API_KEY})

        if gen_response.status_code == 200:
            # The API returns an identifier for the file
            file_id = gen_response.text.strip()
            print(f"Received file ID: {file_id}")

            # 2. Poll for the file to become available
            download_url = f"{BASE_URL}/download?id={file_id}"
            max_attempts = 3  # Total attempts to download file
            attempt = 0
            download_response = None

            while attempt < max_attempts:
                download_response = requests.get(download_url, headers={"apikey": API_KEY})
                if download_response.status_code == 200:
                    print("File is ready for download.")
                    break
                elif download_response.status_code == 429:  # API rate limit exceeded, try again after 1 hour
                    raise Exception("API rate limit exceeded. 15/h is the limit. Try again after 1 hour.")
                else:
                    print("File not ready yet, waiting 2 seconds...")
                    time.sleep(2)
                    attempt += 1

            if attempt == max_attempts:
                print(f"Failed to download file for {formatted_date} and pollutant {pollutant_code} after {max_attempts} attempts.")
                continue

            # 3. Save the downloaded file to S3 with pollutant as the key
            filename = f"polluant-{pollutant_code}_{formatted_date}.csv"
            s3_key = f"{pollutant_code}/{filename}"

            try:
                s3.upload_fileobj(io.BytesIO(download_response.content), BUCKET_NAME, s3_key)
                print(f"File saved to S3: s3://{BUCKET_NAME}/{s3_key}")
            except Exception as e:
                print(f"Error during upload for {formatted_date} and pollutant {pollutant_code}: {e}")
        else:
            print(f"Error generating file for {formatted_date} and pollutant {pollutant_code}: HTTP {gen_response.status_code}")
