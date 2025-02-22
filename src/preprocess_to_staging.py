import os
import re
import csv
import io
import yaml
import unicodedata
from uuid import uuid4
import boto3
from cassandra.cluster import Cluster

def normalize_column_name(col):
    """
    Normalize column names to valid Cassandra identifiers:
      - Remove accents.
      - Convert to lower-case.
      - Replace any non-alphanumeric characters with underscores.
      - If a column starts with a digit, prefix it with an underscore.
    """
    # Remove accents and convert to ascii
    col = unicodedata.normalize('NFKD', col).encode('ASCII', 'ignore').decode('utf-8')
    col = col.lower().strip()
    # Replace non-alphanumeric characters with underscore
    col = re.sub(r'[^a-z0-9]+', '_', col)
    # If the first character is a digit, prefix with underscore
    if col and col[0].isdigit():
        col = "_" + col
    return col

def main():
    # Load configuration files
    with open("config/config.yaml", "r") as file:
        config = yaml.safe_load(file)
    with open("config/pollutants.yaml", "r") as file:
        pollutants = yaml.safe_load(file)

    # Connect to S3 using boto3
    s3 = boto3.client(
        "s3",
        endpoint_url=config["s3"]["endpoint_url"],
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
    )
    bucket_name = config["s3"]["bucket_name"]

    # Connect to Cassandra
    cluster = Cluster([config["cassandra"]["host"]], port=config["cassandra"]["port"])
    session = cluster.connect()
    keyspace = config["cassandra"]["keyspace"]
    
    # Create keyspace if it does not exist (using SimpleStrategy for demo purposes)
    session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {keyspace}
        WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': 1 }}
    """)
    session.set_keyspace(keyspace)

    # Process each pollutant defined in pollutants.yaml
    for pollutant in pollutants:
        pollutant_code = pollutant["code"]
        pollutant_short_name = pollutant["short_name"]
        # Normalize table name using the pollutant short_name
        table_name = normalize_column_name(pollutant_short_name)
        print(f"\n=== Processing pollutant: {pollutant_short_name} (Code: {pollutant_code}) ===")
        
        # List all objects in S3 for the given pollutant folder (prefix is the pollutant code, e.g. "03/")
        prefix = f"{pollutant_code}/"
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        if "Contents" not in response:
            print(f"No files found in S3 for pollutant {pollutant_short_name}.")
            continue

        concatenated_rows = []
        header_normalized = None

        # Process each file found under the folder
        for obj in response["Contents"]:
            key = obj["Key"]
            print(f"Processing file: {key}")
            obj_response = s3.get_object(Bucket=bucket_name, Key=key)
            content = obj_response["Body"].read().decode("utf-8")
            reader = csv.reader(io.StringIO(content), delimiter=';')
            rows = list(reader)
            if not rows:
                continue

            # Normalize header names (only once per pollutant)
            header = rows[0]
            normalized_header = [normalize_column_name(col) for col in header]
            if header_normalized is None:
                header_normalized = normalized_header
                concatenated_rows.append(header_normalized)
            else:
                # If headers do not match, skip this file
                if normalized_header != header_normalized:
                    print(f"Header mismatch in file {key}. Skipping file.")
                    continue

            # Append data rows (skip the header)
            for row in rows[1:]:
                # Ensure row is not empty
                if row and any(cell.strip() for cell in row):
                    concatenated_rows.append(row)

        # Remove duplicate rows (ignoring the header row)
        if len(concatenated_rows) <= 1:
            print(f"No data rows found for pollutant {pollutant_short_name}.")
            continue

        unique_data = [concatenated_rows[0]]  # include header
        seen = set()
        for row in concatenated_rows[1:]:
            t = tuple(row)
            if t not in seen:
                seen.add(t)
                unique_data.append(row)
        print(f"Total unique rows for {pollutant_short_name}: {len(unique_data)-1}")

        # Create table in Cassandra if it doesn't exist.
        # For simplicity, we add an extra column "id" as a synthetic primary key.
        columns = header_normalized
        create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} (" \
                             f"id uuid, " + \
                             ", ".join([f"{col} text" for col in columns]) + \
                             ", PRIMARY KEY (id)" \
                             f");"
        print(f"Creating table {table_name} if not exists...")
        session.execute(create_table_query)

        # Prepare the insert statement.
        # We'll insert the synthetic id along with all CSV columns.
        insert_columns = ", ".join(["id"] + columns)
        placeholders = ", ".join(["?"] * (len(columns) + 1))
        insert_query = f"INSERT INTO {table_name} ({insert_columns}) VALUES ({placeholders});"
        prepared = session.prepare(insert_query)

        # Insert each data row into the table.
        for row in unique_data[1:]:
            row_data = [uuid4()] + row
            session.execute(prepared, row_data)
        print(f"Inserted {len(unique_data)-1} rows into table '{table_name}'.")

if __name__ == "__main__":
    main()
