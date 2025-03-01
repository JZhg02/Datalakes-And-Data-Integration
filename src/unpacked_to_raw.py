import os
import yaml
import requests
import time
import io
import boto3
from datetime import datetime, timedelta


def create_bucket_if_not_exists(s3_client, bucket_name):
    """
    Check if bucket exists, create it if not.
    """
    buckets = s3_client.list_buckets().get("Buckets", [])
    bucket_names = [bucket["Name"] for bucket in buckets]
    
    if bucket_name in bucket_names:
        print(f"Bucket '{bucket_name}' already exists.")
    else:
        s3_client.create_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' created successfully!")


def generate_date_range(days):
    """
    Generate a list of dates for the last N days.
    """
    today = datetime.today()
    return [(today - timedelta(days=i + 1)).strftime("%Y-%m-%d") for i in range(days)]


def request_file_generation(base_url, api_key, date, pollutant_code):
    """
    Request generation of statistics file for a given date and pollutant.
    """
    export_url = f"{base_url}/MoyH/export?date={date}&polluant={pollutant_code}"
    print(f"Requesting file generation via: {export_url}")
    
    response = requests.get(export_url, headers={"apikey": api_key})
    
    if response.status_code == 200:
        file_id = response.text.strip()
        print(f"Received file ID: {file_id}")
        return file_id
    else:
        print(f"Error generating file: HTTP {response.status_code}")
        return None


def download_file(base_url, api_key, file_id, initial_delay=5, max_attempts=5, wait_between_attempts=2):
    """
    Wait for an initial delay, then poll for file availability and download when ready.
    """
    download_url = f"{base_url}/download?id={file_id}"
    
    # Wait before starting to poll the API (function is called multiple times and file generation from API side takes time)
    print(f"Waiting for {initial_delay} seconds before starting checks...")
    time.sleep(initial_delay)
    
    attempt = 0
    while attempt < max_attempts:
        response = requests.get(download_url, headers={"apikey": api_key})
        
        # Try to parse the response as JSON
        try:
            data = response.json()
        except ValueError:
            data = None

        # Check if the JSON contains a 'status' field
        if data and "status" in data:
            if data["status"] == 412:
                print("File not ready yet, waiting 2 seconds before next check...")
                time.sleep(wait_between_attempts)
                attempt += 1
                continue
            elif data["status"] == 429:
                raise Exception("API rate limit exceeded. 15/h is the limit. Try again after 1 hour.")

        # If no error status is detected, assume the file is ready for download.
        print("File is ready for download.")
        print(response)
        print(response.content[:100])
        return response.content

    print(f"Failed to download file after {max_attempts} attempts.")
    return None


def upload_to_s3(s3_client, bucket_name, content, s3_key):
    """
    Upload content to S3 bucket.
    """
    try:
        s3_client.upload_fileobj(io.BytesIO(content), bucket_name, s3_key)
        print(f"File saved to S3: s3://{bucket_name}/{s3_key}")
        return True
    except Exception as e:
        print(f"Error during upload: {e}")
        return False


def process_pollutant_data(base_url, api_key, s3_client, bucket_name, date, pollutant):
    """
    Process data for a single pollutant on a specific date.
    """
    pollutant_code = pollutant["code"]
    pollutant_short_name = pollutant["short_name"]
    print(f"\nProcessing pollutant: {pollutant_short_name} (Code: {pollutant_code})")
    
    # Request file generation
    file_id = request_file_generation(base_url, api_key, date, pollutant_code)
    if not file_id:
        return False
    
    # Download file
    file_content = download_file(base_url, api_key, file_id)
    if not file_content:
        return False
    
    # Upload to S3
    filename = f"polluant-{pollutant_code}_{date}.csv"
    s3_key = f"{pollutant_code}/{filename}"
    return upload_to_s3(s3_client, bucket_name, file_content, s3_key)


def main():
    # Load configurations
    with open("config/config.yaml", "r") as file:
        config = yaml.safe_load(file)
    with open("config/pollutants.yaml", "r") as file:
        pollutants = yaml.safe_load(file)
    
    # Get GEODAIR API key and other environment variables
    api_key = os.getenv("GEODAIR_API_KEY")
    if not api_key:
        raise ValueError("GEODAIR_API_KEY is not set in the environment.")
    
    aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    
    # Get configuration parameters
    base_url = config["data_source"]["base_url"]
    last_n_days = config["data_source"]["last_n_days"]
    bucket_name = config["s3"]["bucket_name"]
    
    # Connect to S3
    s3 = boto3.client(
        "s3",
        endpoint_url=config["s3"]["endpoint_url"],
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )
    bucket_name = config["s3"]["bucket_name"]

    # Create bucket if it does not exist
    create_bucket_if_not_exists(s3, bucket_name)
    
    # Generate date range
    dates = generate_date_range(last_n_days)
    
    # Process data for each date and pollutant
    for date in dates:
        print(f"\nProcessing data for date: {date}")
        
        for pollutant in pollutants:
            process_pollutant_data(
                base_url, 
                api_key, 
                s3, 
                bucket_name, 
                date, 
                pollutant
            )


if __name__ == "__main__":
    main()
    # to test if data has been saved to s3 enter the following command on the terminal
    # aws --endpoint-url=http://localhost:4566 s3 ls raw --recursive