import os
import yaml
import pandas as pd
import time
from cassandra.cluster import Cluster
from sqlalchemy import create_engine


def list_tables(session, keyspace):
    """
    List all tables in the given keyspace.
    """
    # For Cassandra 3.x+, table metadata is stored in system_schema.tables.
    query = f"SELECT table_name FROM system_schema.tables WHERE keyspace_name = '{keyspace}'"
    rows = session.execute(query)
    table_names = [row.table_name for row in rows]
    return table_names


def load_table_to_dataframe(session, table_name):
    """
    Load all rows from a Cassandra table into a pandas DataFrame.
    """
    query = f"SELECT * FROM {table_name}"
    rows = session.execute(query)
    # Create DataFrame directly from list of rows.
    df = pd.DataFrame(list(rows))
    return df


def process_table_dataframe(df, table_name):
    """
    Process the DataFrame from a table:
      - If the 'valeur' column exists, drop rows where 'valeur' is empty (None/NaN).
      - Drop the unwanted columns 'date_de_fin' and 'polluant'.
      - Rename the remaining columns (except 'code_site' and 'date_de_debut') to include the table name as a prefix.
      - Group the DataFrame by ('code_site', 'date_de_debut') using the first available record.
    """
    # If 'valeur' exists, filter out rows with empty 'valeur'
    if 'valeur' in df.columns:
        df = df[df['valeur'].notna()]

    # Drop unwanted columns 'date_de_fin' and 'polluant' if they exist
    df = df.drop(columns=[col for col in ['date_de_fin', 'polluant'] if col in df.columns], errors='ignore')
    
    # Rename columns by prefixing with the table name (except for the grouping keys)
    rename_dict = {
        col: f"{table_name}_{col}"
        for col in df.columns if col not in ['code_site', 'date_de_debut']
    }
    df = df.rename(columns=rename_dict)
    
    # Group by the common keys to ensure one row per (code_site, date_de_debut)
    # Here we use the first available record in each group.
    df = df.groupby(['code_site', 'date_de_debut'], as_index=False).first()
    return df


def merge_dataframes(dfs):
    """
    Merge a list of DataFrames on the common keys (code_site, date_de_debut) using an outer join.
    """
    if not dfs:
        return pd.DataFrame()
    
    merged_df = dfs[0]
    for df in dfs[1:]:
        merged_df = pd.merge(merged_df, df, on=['code_site', 'date_de_debut'], how='outer')
    return merged_df


def ingest_dataframe_to_postgres(df, table_name, pg_config):
    """
    Ingest the given DataFrame into PostgreSQL.
    """
    # Build the SQLAlchemy engine connection string.
    engine_str = (
        f"postgresql+psycopg2://{pg_config['user']}:{pg_config['password']}"
        f"@{pg_config['host']}:{pg_config['port']}/{pg_config['database']}"
    )
    engine = create_engine(engine_str)
    # Ingest into the specified table (here, we use 'replace' to overwrite existing data).
    df.to_sql(table_name, engine, if_exists='replace', index=False)
    print(f"Data ingested into PostgreSQL table '{table_name}'.")


def main():

    start_time = time.time()

    # Load configuration (ensure config/config.yaml exists)
    with open("config/config.yaml", "r") as f:
        config = yaml.safe_load(f)
    
    # Connect to Cassandra
    cluster = Cluster([config["cassandra"]["host"]], port=config["cassandra"]["port"])
    session = cluster.connect()
    keyspace = config["cassandra"]["keyspace"]
    session.set_keyspace(keyspace)
    
    # List all tables in the keyspace.
    table_names = list_tables(session, keyspace)
    if not table_names:
        print("No tables found in Cassandra for keyspace:", keyspace)
        return
    
    processed_dfs = []
    for table in table_names:
        print(f"Processing table: {table}")
        df = load_table_to_dataframe(session, table)
        if df.empty:
            print(f"Table '{table}' is empty. Skipping.")
            continue
        
        # Process each DataFrame (including filtering out rows with empty 'valeur')
        df_processed = process_table_dataframe(df, table)
        if df_processed.empty:
            print(f"After filtering, table '{table}' has no rows. Skipping.")
            continue
        processed_dfs.append(df_processed)
    
    if not processed_dfs:
        print("No data loaded from any table after filtering.")
        return

    # Merge all processed DataFrames on code_site and date_de_debut.
    merged_df = merge_dataframes(processed_dfs)
    
    # Display the merged (curated) DataFrame.
    print("Curated DataFrame:")
    print(merged_df.head())
    print("Shape of the final DataFrame:", merged_df.shape)

    # Create a second DataFrame: filter rows where all columns ending with '_valeur' are non-null.
    valeur_columns = [col for col in merged_df.columns if col.endswith('_valeur')]
    if valeur_columns:
        df_all_valeur = merged_df.dropna(subset=valeur_columns)
        print("\nDataFrame with all '_valeur' columns having values:")
        print(df_all_valeur.head())
        print("Shape of the DataFrame with all '_valeur' columns:", df_all_valeur.shape)
    else:
        print("\nNo columns ending with '_valeur' were found in the merged DataFrame.")

    # Load PostgreSQL configuration
    pg_config = config["postgresql"]
    # Add postgres_user and postgres_password to pg_config from .env
    pg_config["user"] = os.getenv("POSTGRES_USER")
    pg_config["password"] = os.getenv("POSTGRES_PASSWORD")
    print("PostgreSQL configuration:", pg_config)
    
    # Ingest the final curated DataFrame into PostgreSQL into a table called "curated".
    ingest_dataframe_to_postgres(merged_df, "curated", pg_config)

    # Ingest the second DataFrame into a different table called "curated_all_valeur".
    ingest_dataframe_to_postgres(df_all_valeur, "curated_all_valeur", pg_config)

    print(f"Total time taken: {(time.time() - start_time) / 60} minutes.")

if __name__ == "__main__":
    main()
