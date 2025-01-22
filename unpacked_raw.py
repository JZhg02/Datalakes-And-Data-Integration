import os
import requests
import zipfile
import io
import pandas as pd
from datetime import datetime
import boto3


# Define the bucket name
bucket_name = "raw"
# Public API URL
url = "https://transport.data.gouv.fr/api/datasets/64635525318cc75a9a8a771f"


# Connect to LocalStack S3
s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:4566",  # LocalStack endpoint
    aws_access_key_id="test", 
    aws_secret_access_key="test"
)


# Fetch data from API
response = requests.get(url)
if response.status_code == 200:
    data = response.json()
else:
    print(f"Erreur : {response.status_code}")
    data = None


# Check if the bucket exists
buckets = s3.list_buckets().get("Buckets", [])
bucket_names = [bucket["Name"] for bucket in buckets]

if bucket_name in bucket_names:
    print(f"Bucket '{bucket_name}' already exists.")
else:
    # Create the bucket if it doesn't exist
    s3.create_bucket(Bucket=bucket_name)
    print(f"Bucket '{bucket_name}' created successfully!")


# Check if data is available
if data:
    # Process each entry in the history
    for entry in data["history"]:
        payload = entry.get("payload", {})
        zip_metadata = payload.get("zip_metadata", [])
        resource_url = payload.get("resource_latest_url")

        print(f"Download from : {resource_url}")

        # Download ZIP file
        response = requests.get(resource_url)
        if response.status_code == 200:
            with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                for file_meta in zip_metadata: # Process each file in the ZIP
                    file_name = file_meta["file_name"]
                    last_modified_datetime = file_meta["last_modified_datetime"]

                    # Convert "last_modified_datetime" to YYYY-MM-DD format
                    last_modified_date = datetime.strptime(last_modified_datetime, "%Y-%m-%dT%H:%M:%S").strftime("%Y-%m-%d")

                    print(f"Extraction of {file_name} from {last_modified_date} date.")

                    with z.open(file_name) as file:
                        try:
                            # Create a 'folder' structure in S3 based on the file name
                            folder_name = file_name.replace(".txt", "")  # Remove .txt to create folder name
                            s3_key = f"{folder_name}/{folder_name}_{last_modified_date}.csv"  # S3 key with folder structure

                            # Upload to S3
                            s3.upload_fileobj(file, bucket_name, s3_key)
                            print(f"{file_name} saved.")

                        except Exception as e:
                            print(f"Error during upload: {e}")

        else:
            print(f"Error during download : {response.status_code}")

else:
    print("No data available.")
