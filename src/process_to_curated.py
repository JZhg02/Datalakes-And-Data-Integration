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


def convert_units(df, table_name):
    """
    Convert specific pollutant values from their given units to g/L.
    """

    unit_conversion = {
        "mg-m3": 1e-3,  # Milligrams per cubic meter to g/L
        "µg-m3": 1e-6,  # Micrograms per cubic meter to g/L
        "ng-m3": 1e-9,  # Nanograms per cubic meter to g/L
    }

    # Convert the specified pollutant values to g/L
    # Table_name is the name of the table from which the DataFrame was loaded
    for unit, factor in unit_conversion.items():
        if f"{table_name}_valeur" in df.columns:
            df[f"{table_name}_valeur_g_par_L"] = df[f"{table_name}_valeur"] * factor
        if f"{table_name}_valeur_brute" in df.columns:
            df[f"{table_name}_valeur_brute_g_par_L"] = df[f"{table_name}_valeur_brute"] * factor

    return df


def aggregate_valeurs(df):
    """
    Aggregate all _valeur and _valeur_brute columns into total_valeur_particule.
    Ensures numeric values before summing.
    """
    # Retrieve all columns ending with '_valeur' and '_valeur_brute' and does not end in '_type_de_valeur'
    valeur_columns = [col for col in df.columns if col.endswith("_valeur") and not col.endswith("_type_de_valeur")]
    valeur_brute_columns = [col for col in df.columns if col.endswith("_valeur_brute")]
    cols = valeur_columns + valeur_brute_columns

    # Using pandas sum method
    # df["total_valeur_particule"] = df[cols].sum(axis=1, skipna=True)

    # Iterate over each row in the DataFrame and compute the sum of the specified columns
    total_valeur_particule = []
    for idx in df.index:
        row_sum = 0  # Initialize the sum for the current row
        for col in cols:
            value = df.at[idx, col]  # Access the value at the given row and column
            if pd.notnull(value):    # Only add if the value is not NaN
                row_sum += value
        total_valeur_particule.append(row_sum)

    # Assign the computed sums as a new column in the DataFrame
    df["total_valeur_particule"] = total_valeur_particule

    return df


def process_table_dataframe(df, table_name):
    """
    Process the DataFrame:
      - Filter out rows where 'valeur' is empty.
      - Drop unwanted columns.
      - Rename columns with table name prefix.
      - Convert units for specified pollutants.
      - Aggregate values.
      - Group by (code_site, date_de_debut).
    """
    # If 'valeur' exists, filter out rows with empty 'valeur'
    if 'valeur' in df.columns:
        df = df[df['valeur'].notna()]

    # Drop unwanted columns 'date_de_fin' and 'polluant' if they exist
    df = df.drop(columns=[col for col in ['date_de_fin', 'polluant'] if col in df.columns], errors='ignore')
    
    # Rename columns by prefixing with the table name (except for the grouping keys)
    rename_dict = {col: f"{table_name}_{col}" for col in df.columns if col not in ['code_site', 'date_de_debut']}
    df = df.rename(columns=rename_dict)

    df = convert_units(df, table_name)

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
    
    # Load each table into a DataFrame and process it
    processed_dfs = []
    for table in table_names:
        print(f"Processing table: {table}")
        df = load_table_to_dataframe(session, table)
        if df.empty:
            print(f"Table '{table}' is empty. Skipping.")
            continue
        
        # Process each DataFrame and append to the list of processed DataFrames
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

    # Aggregate all _valeur and _valeur_brute columns into total_valeur_particule and total_valeur_brute_particule respectively.
    merged_df = aggregate_valeurs(merged_df)
    
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
