import os
import re
import csv
import io
import yaml
import unicodedata
import time
import datetime
import boto3
from cassandra.cluster import Cluster


def normalize_column_name(col):
    """
    Normalize column names to valid Cassandra identifiers:
      - Remove accents
      - Convert to lower-case
      - Replace any non-alphanumeric characters with underscores
      - If a column starts with a digit, prefix it with an underscore
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


def convert_value(col_name, value):
    """
    Convert CSV string values to the appropriate Python types based on the column.
    """
    if value == "" or value is None:
        return None

    # Convert date columns to datetime objects.
    if col_name in ('date_de_debut', 'date_de_fin'):
        # Try parsing with a datetime format. Adjust the format if needed.
        try:
            # If a time component is present:
            if " " in value:
                return datetime.datetime.strptime(value, "%Y/%m/%d %H:%M:%S")
            else:
                return datetime.datetime.strptime(value, "%Y/%m/%d")
        except Exception as e:
            print(f"Error parsing date for column {col_name} with value '{value}': {e}")
            return None

    # Convert numeric fields to float
    elif col_name in ('valeur', 'valeur_brute', 'taux_de_saisie'):
        try:
            return float(value)
        except Exception as e:
            print(f"Error converting float for column {col_name} with value '{value}': {e}")
            return None

    return value


def create_cassandra_keyspace(session, keyspace):
    """
    Create a Cassandra keyspace if it does not exist.
    """
    # SimpleStrategy and replication factor 1 for demo
    session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {keyspace}
        WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': 1 }} 
    """)
    session.set_keyspace(keyspace)


def create_cassandra_table(session, table_name):
    """
    Create a Cassandra table with the specified columns.
    """
    create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            date_de_debut timestamp,
            date_de_fin timestamp,
            organisme text,
            code_zas text,
            zas text,
            code_site text,
            nom_site text,
            type_d_implantation text,
            polluant text,
            type_d_influence text,
            discriminant text,
            reglementaire text, 
            type_d_evaluation text,
            procedure_de_mesure text,
            type_de_valeur text,
            valeur float, 
            valeur_brute float, 
            unite_de_mesure text,
            taux_de_saisie float, 
            couverture_temporelle text,
            couverture_de_donnees text,
            code_qualite text,
            validite text, 
            PRIMARY KEY (code_site, date_de_debut) 
        ) WITH CLUSTERING ORDER BY (date_de_debut ASC);
    """
    session.execute(create_table_query)


def get_s3_files(s3, bucket_name, prefix):
    """
    Get list of files from S3 with the given prefix/key/particle_code.
    """
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    if "Contents" not in response:
        return []
    return response["Contents"]


def process_s3_file(s3, bucket_name, key):
    """
    Read and parse a CSV file from S3 bucket.
    """
    obj_response = s3.get_object(Bucket=bucket_name, Key=key)
    content = obj_response["Body"].read().decode("utf-8")
    reader = csv.reader(io.StringIO(content), delimiter=';')
    return list(reader)


def insert_data_into_cassandra(session, table_name, columns, data):
    """
    Insert data rows into the Cassandra table after converting values.
    """
    # Prepare the insert query
    insert_columns = ", ".join(columns)
    placeholders = ", ".join(["?"] * len(columns)) # Prepare placeholders for the prepared statement : (?, ?, ?, ...)
    insert_query = f"INSERT INTO {table_name} ({insert_columns}) VALUES ({placeholders}) IF NOT EXISTS"
    prepared = session.prepare(insert_query)
    
    for row in data:
        # Convert each value in the row using the column name
        converted_row = [convert_value(col, value) for col, value in zip(columns, row)]
        session.execute(prepared, converted_row)


def process_pollutant(pollutant, s3, session, bucket_name):
    """
    Process a single pollutant's data files.
    """
    pollutant_code = pollutant["code"]
    pollutant_short_name = pollutant["short_name"]
    table_name = normalize_column_name(pollutant_short_name)
    
    print(f"\n=== Processing pollutant: {pollutant_short_name} (Code: {pollutant_code}) ===")
    
    # List all objects in S3 for the given pollutant folder/key
    prefix = f"{pollutant_code}/"
    files = get_s3_files(s3, bucket_name, prefix)
    
    if not files:
        print(f"No files found in S3 for pollutant {pollutant_short_name}.")
        return
    
    # Process all files and collect data
    all_rows = []
    header_normalized = None

    seen = set() # Rows already seen (to avoid duplicates)
    
    for obj in files:
        key = obj["Key"]
        print(f"Processing file: {key}")
        rows = process_s3_file(s3, bucket_name, key)
        
        if not rows:
            continue
        
        # Normalize header names
        header = rows[0]
        normalized_header = [normalize_column_name(col) for col in header]
        
        # Initialize header if first file or validate header if not
        if header_normalized is None:
            header_normalized = normalized_header
            all_rows.append(header_normalized)
        elif normalized_header != header_normalized: # Should match the first file's header but just in case
            print(f"Header mismatch in file {key}. Skipping file.")
            continue
        
        # Add non-empty data rows and skip duplicates
        for row in rows[1:]:
            if row and any(cell.strip() for cell in row) and tuple(row) not in seen:
                seen.add(tuple(row))
                all_rows.append(row)
    
    # Check if we have any data
    if len(all_rows) <= 1:
        print(f"No data rows found for pollutant {pollutant_short_name}.")
        return
    
    print(f"Total rows for {pollutant_short_name}: {len(all_rows)-1}")
    
    # Create Cassandra table and insert data
    create_cassandra_table(session, table_name)
    print(f"Created table {table_name}.")
    
    insert_data_into_cassandra(session, table_name, header_normalized, all_rows[1:])
    print(f"Insertion into table '{table_name}' done.")


def main():
    
    start_time = time.time()

    # Load config variables from config/config.yaml
    with open("config/config.yaml", "r") as f:
        config = yaml.safe_load(f)
    with open("config/pollutants.yaml", "r") as f:
        pollutants = yaml.safe_load(f)

    # Get AWS credentials from environment variables
    aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")

    # Connect to s3
    s3 = boto3.client(
        "s3",
        endpoint_url=config["s3"]["endpoint_url"],
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )
    bucket_name = config["s3"]["bucket_name"]

    # Connect to Cassandra
    cluster = Cluster([config["cassandra"]["host"]], port=config["cassandra"]["port"])
    session = cluster.connect()
    keyspace = config["cassandra"]["keyspace"]
    # Create keyspace if it does not exist
    create_cassandra_keyspace(session, keyspace)

    # Process each pollutant
    for pollutant in pollutants:
        print("pollutant: ", pollutant)
        process_pollutant(pollutant, s3, session, bucket_name)
    
    elapsed_time = time.time() - start_time
    print(f"Total time taken: {elapsed_time // 60} minutes and {elapsed_time % 60} seconds.")

if __name__ == "__main__":
    main()
