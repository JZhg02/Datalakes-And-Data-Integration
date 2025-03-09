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
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading


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


def batch_insert_data_into_cassandra(session, table_name, columns, data, batch_size=100):
    """
    Insert data rows into the Cassandra table in batches.
    
    Args:
        table_name: Target table name
        columns: List of column names
        data: List of data rows to insert
        batch_size: Number of rows to insert in each batch
    """
    # Prepare the insert query
    insert_columns = ", ".join(columns)
    placeholders = ", ".join(["?"] * len(columns))
    insert_query = f"INSERT INTO {table_name} ({insert_columns}) VALUES ({placeholders}) IF NOT EXISTS"
    prepared = session.prepare(insert_query)
    
    # Process in batches to improve performance
    total_rows = len(data)
    
    for i in range(0, total_rows, batch_size):
        # Calculate the end index for current batch
        batch_end = min(i + batch_size, total_rows)
        batch = data[i:batch_end]
        
        # Process each row in the current batch
        for row in batch:
            # Convert each value in the row using the column name
            converted_row = [convert_value(col, value) for col, value in zip(columns, row)]
            session.execute(prepared, converted_row)
        
        # Log progress at regular intervals or at the end
        if (i + batch_size) % 2000 == 0 or batch_end == total_rows:
            print(f"  - Inserted {batch_end}/{total_rows} rows into {table_name}")


def process_s3_files_parallel(files, s3, bucket_name, max_workers=4):
    """
    Process S3 files in parallel using thread pool.
    
    Args:
        files: List of S3 file objects to process
        s3: S3 client
        bucket_name: S3 bucket name
        max_workers: Maximum number of worker threads to use
        
    Returns:
        Tuple of (normalized_header, all_rows)
    """
    header_normalized = None
    all_rows = []
    # Use a set to track seen rows for deduplication across threads
    seen = set()
    
    # Create a lock for thread-safe operations on shared data structures (prevents race conditions)
    lock = threading.Lock()
    
    def process_file(file_obj):
        """
        Inner function to process a single S3 file in a worker thread.
        Returns the normalized header and unique rows from this file.
        """
        key = file_obj["Key"]
        try:
            # Download and parse the file
            rows = process_s3_file(s3, bucket_name, key)
            if not rows:
                return None, []
            
            # Normalize header names
            header = rows[0]
            normalized_header = [normalize_column_name(col) for col in header]
            
            # Filter out duplicate rows within this file
            unique_rows = []
            for row in rows[1:]:
                if row and any(cell.strip() for cell in row):
                    row_tuple = tuple(row)
                    # Use lock to safely check and update the seen set
                    with lock:
                        if row_tuple not in seen:
                            seen.add(row_tuple)
                            unique_rows.append(row)
            
            return normalized_header, unique_rows
        except Exception as e:
            print(f"Error processing file {key}: {e}")
            return None, []
    
    # Create a ThreadPoolExecutor with the specified number of workers
    # ThreadPoolExecutor manages a pool of worker threads for concurrent execution
    # This is ideal for I/O-bound tasks like file processing
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all file processing tasks to the executor
        # Each file will be processed in a separate thread from the pool
        futures = [executor.submit(process_file, file_obj) for file_obj in files]
        
        # Process results as they complete
        # as_completed yields futures as they finish, regardless of submission order
        # This allows us to process results as soon as they're available
        for future in as_completed(futures):
            result_header, result_rows = future.result()
            
            if result_header is None:
                continue
                
            # Thread-safe update of the shared result variables
            with lock:
                if header_normalized is None:
                    # First valid result sets the header standard
                    header_normalized = result_header
                    all_rows = result_rows
                elif result_header == header_normalized:
                    # If header matches our standard, add the rows
                    all_rows.extend(result_rows)
    
    return header_normalized, all_rows


def process_file_batches(s3, bucket_name, file_batch, pollutant_short_name):
    """
    Process a batch of files for a pollutant.
    
    This function is designed to be called by a worker thread to process
    a subset of files for a given pollutant.
    
    Args:
        s3: S3 client
        bucket_name: S3 bucket name
        file_batch: List of file objects to process in this batch
        pollutant_short_name: Short name of the pollutant
        
    Returns:
        Tuple of (normalized_header, batch_results)
    """
    batch_results = []
    header_normalized = None
    seen = set()
    
    for obj in file_batch:
        key = obj["Key"]
        print(f"  - Processing file: {key}")
        
        try:
            rows = process_s3_file(s3, bucket_name, key)
            
            if not rows:
                continue
            
            # Normalize header names
            header = rows[0]
            normalized_header = [normalize_column_name(col) for col in header]
            
            if header_normalized is None:
                header_normalized = normalized_header
            elif normalized_header != header_normalized:
                print(f"Header mismatch in file {key}. Skipping file.")
                continue
            
            # Add non-empty data rows and skip duplicates
            batch_data = []
            for row in rows[1:]:
                if row and any(cell.strip() for cell in row) and tuple(row) not in seen:
                    seen.add(tuple(row))
                    batch_data.append(row)
            
            batch_results.extend(batch_data)
            
        except Exception as e:
            print(f"  - Error processing file {key}: {e}")
    
    return header_normalized, batch_results


def process_pollutant_parallel(pollutant, s3, cluster, bucket_name, keyspace, max_workers=4, batch_size=100):
    """
    Process a single pollutant's data files in parallel.
    
    This function handles the complete parallel processing pipeline for one pollutant:
    1. Creates a dedicated Cassandra session
    2. Lists all S3 files for the pollutant
    3. Processes files in parallel using multiple threads
    4. Inserts the results into Cassandra
    
    Args:
        pollutant: Pollutant configuration dictionary (check config/pollutants.yaml)
        s3: S3 client
        cluster: Cassandra cluster connection
        bucket_name: S3 bucket name
        keyspace: Cassandra keyspace name
        max_workers: Maximum number of worker threads for file processing
        batch_size: Number of rows to insert in each Cassandra batch
    """
    pollutant_code = pollutant["code"]
    pollutant_short_name = pollutant["short_name"]
    table_name = normalize_column_name(pollutant_short_name)
    
    print(f"\n=== Processing pollutant: {pollutant_short_name} (Code: {pollutant_code}) ===")
    
    # Create a dedicated session for this pollutant processing
    # This avoids contention with other pollutant processing threads
    session = cluster.connect(keyspace)
    
    # List all objects in S3 for the given pollutant folder/key
    prefix = f"{pollutant_code}/"
    files = get_s3_files(s3, bucket_name, prefix)
    
    if not files:
        print(f"No files found in S3 for pollutant {pollutant_short_name}.")
        return
    
    print(f"Found {len(files)} files for pollutant {pollutant_short_name}.")
    
    # Create Cassandra table
    create_cassandra_table(session, table_name)
    print(f"Created table {table_name}.")
    
    # Process files in parallel using the thread pool
    # Distributing file processing across multiple threads to maximize I/O throughput
    header_normalized, all_rows = process_s3_files_parallel(files, s3, bucket_name, max_workers)
    
    if not header_normalized or not all_rows:
        print(f"No data rows found for pollutant {pollutant_short_name}.")
        return
    
    print(f"Total unique rows for {pollutant_short_name}: {len(all_rows)}")
    
    # Insert data in batches
    # This improves throughput by reducing the number of network round-trips while still showing progress updates
    batch_insert_data_into_cassandra(session, table_name, header_normalized, all_rows, batch_size)
    print(f"Insertion into table '{table_name}' done.")


def main():
    start_time = time.time()

    # Load config variables from config/config.yaml
    with open("config/config.yaml", "r") as f:
        config = yaml.safe_load(f)
    with open("config/pollutants.yaml", "r") as f:
        pollutants = yaml.safe_load(f)

    # Get AWS credentials from config
    aws_access_key_id = config["s3"]["aws_access_key_id"]
    aws_secret_access_key = config["s3"]["aws_secret_access_key"]

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
    
    # Set the number of worker threads
    max_workers = 4 # (for first level parallelism: processing multiple pollutants)
    batch_size = 100 
    
    # Process pollutants in parallel using a ThreadPoolExecutor
    # - Each pollutant is processed in its own thread
    # - Each pollutant's files are processed in parallel within that thread
    # - This creates a two-level parallel execution structure
    with ThreadPoolExecutor(max_workers=min(len(pollutants), max_workers)) as executor:
        # Submit each pollutant processing as a separate task
        # limit max_workers to the number of pollutants to avoid creating unnecessary threads
        futures = [executor.submit(
            process_pollutant_parallel, 
            pollutant, 
            s3, 
            cluster, 
            bucket_name, 
            keyspace, 
            max_workers=2,  # Workers per pollutant processing (second-level parallelism : processing multiple files)
            batch_size=batch_size
        ) for pollutant in pollutants]
        
        # Wait for all tasks to complete and handle any exceptions as_completed yields futures as they finish, regardless of submission order
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Error in pollutant processing: {e}")
    
    # Calculate and report the total execution time
    elapsed_time = time.time() - start_time
    print(f"Total time taken: {elapsed_time // 60} minutes and {elapsed_time % 60:.2f} seconds.")
    print(f"Parallelized processing complete.")

if __name__ == "__main__":
    main()