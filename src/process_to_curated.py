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
    # DataFrame from list of rows.
    df = pd.DataFrame(list(rows))
    return df


def convert_units(df):
    """
    Convert pollutant values from their given units to g/L based on the unit column.
    """
    # Define conversion factors for each unit.
    unit_conversion = {
        "mg-m3": 1e-3,  # Milligrams per cubic meter to g/L
        "µg-m3": 1e-6,  # Micrograms per cubic meter to g/L
        "ng-m3": 1e-9,  # Nanograms per cubic meter to g/L
    }
    
    # Detect unit columns dynamically
    unit_columns = [col for col in df.columns if col.endswith("_unite_de_mesure")]
    
    for unit_column in unit_columns:
        table_prefix = unit_column.replace("_unite_de_mesure", "")
        
        # Check if unit column has any missing values
        if df[unit_column].isnull().any():
            # Forward-fill missing values in unit column
            df.loc[:, unit_column] = df[unit_column].ffill()
        # Recheck if there are any missing values
        if df[unit_column].isnull().any():
            # Backward-fill missing values in unit column
            df.loc[:, unit_column] = df[unit_column].bfill()
        
        # Create a conversion factor column
        df[f"{table_prefix}_conversion_factor"] = df[unit_column].map(unit_conversion)
        
        # Convert value columns
        for value_suffix in ["_valeur", "_valeur_brute"]:
            value_col = f"{table_prefix}{value_suffix}"
            if value_col in df.columns:
                df[f"{value_col}_g_par_L"] = df[value_col] * df[f"{table_prefix}_conversion_factor"]
        
        # Remove the temporary conversion factor column
        df.drop(columns=[f"{table_prefix}_conversion_factor"], inplace=True)
    
    return df


def aggregate_valeurs(df):
    """
    Aggregate all columns ending with '_valeur_g_par_L' (excluding those ending with '_type_de_valeur')
    and columns ending with '_valeur_brute_g_par_L' into a new column 'total_valeur_particule_g_par_L' using NumPy.
    """
    valeur_columns = [col for col in df.columns if col.endswith("_valeur_g_par_L") and not col.endswith("_type_de_valeur")]
    valeur_brute_columns = [col for col in df.columns if col.endswith("_valeur_brute_g_par_L")]
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
    df["total_valeur_particule_g_par_L"] = total_valeur_particule
    return df


def replace_null_values_by_mean(df):
    """
    Replace null numeric values with the mean of their respective column.
    """
    numeric_cols = df.select_dtypes(include='number').columns
    for col in numeric_cols:
        mean_val = df[col].mean()
        df[col] = df[col].fillna(mean_val)
    return df


def shift_and_calculate_diff_6_hours_ago(df):
    """
    Shift the values by 6 hours and calculate the difference between the current value
    and the value 6 hours ago for each '_valeur' column
    Replace NaNs (due to the shifting) with the original value so that difference = 0.
    """
    # Identify all '_valeur' columns, excluding those that end with '_type_de_valeur'
    value_columns = [col for col in df.columns if col.endswith('_valeur') and not col.endswith('_type_de_valeur') or col == 'total_valeur_particule_g_par_L']
    
    for value_col in value_columns:
        # Ensure the column is numeric, coerce errors to NaN
        df[value_col] = pd.to_numeric(df[value_col], errors='coerce')

        # Shift the values by 6 hours (assuming the date_de_debut column is sorted by time)
        shifted_col = df[value_col].shift(6)

        # Replace NaNs in the shifted column with corresponding values from the original column
        shifted_col.fillna(df[value_col], inplace=True)

        # Calculate the difference between the current value and the shifted value
        diff_col = df[value_col] - shifted_col
        
        # Add the new difference column to the dataframe
        df[f"{value_col}_diff_6hrs"] = diff_col
    
    return df


def calculate_particle_variation(df):
    """
    Shift the values by 6 hours and calculate the percentage change between the current value and the value 6 hours ago for each '_valeur' column
    """
    # Identify all '_valeur' columns, excluding those that end with '_type_de_valeur'
    value_columns = [col for col in df.columns if col.endswith('_valeur') and not col.endswith('_type_de_valeur') or col == 'total_valeur_particule_g_par_L']
    
    for value_col in value_columns:
        # Ensure the column is numeric, coerce errors to NaN
        df[value_col] = pd.to_numeric(df[value_col], errors='coerce')

        # Shift the values by 6 hours (assuming the date_de_debut column is sorted by time)
        shifted_col = df[value_col].shift(6)

        # Calculate the percentage change between the current value and the shifted value
        percentage_change_col = ((df[value_col] - shifted_col) / shifted_col) * 100

        # Add the new difference and percentage change columns to the dataframe
        df[f"{value_col}_percent_change_6hrs"] = percentage_change_col

    return df


def drop_unwanted_col_and_add_table_prefix(df, table_name):
    """
    Process the DataFrame for one table:
      - Drop unwanted columns.
      - Rename columns by prefixing them with the table name (except for the grouping keys).
    """
    # Drop unwanted columns if they exist.
    df = df.drop(columns=[col for col in ['date_de_fin', 'polluant'] if col in df.columns], errors='ignore')
    
    # Rename columns by adding the table name as prefix.
    rename_dict = {col: f"{table_name}_{col}" for col in df.columns if col not in ['code_site', 'date_de_debut']}
    df = df.rename(columns=rename_dict)
    
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
    engine_str = (
        f"postgresql+psycopg2://{pg_config['postgres_user']}:{pg_config['postgres_password']}"
        f"@{pg_config['host']}:{pg_config['port']}/{pg_config['database']}"
    )
    engine = create_engine(engine_str)
    df.to_sql(table_name, engine, if_exists='replace', index=False)
    print(f"Data ingested into PostgreSQL table '{table_name}'.")


def main():
    start_time = time.time()

    # Load configuration.
    with open("config/config.yaml", "r") as f:
        config = yaml.safe_load(f)
    
    # Connect to Cassandra.
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
    processed_table_names = [] 
    
    for table in table_names:
        print(f"Processing table: {table}")
        df = load_table_to_dataframe(session, table)
        if df.empty:
            print(f"Table '{table}' is empty. Skipping.")
            continue
        
        # Preprocess by adding table prefix 
        df_processed = drop_unwanted_col_and_add_table_prefix(df, table)
        if df_processed.empty:
            print(f"After processing, table '{table}' has no rows. Skipping.")
            continue
        
        processed_dfs.append(df_processed)
        processed_table_names.append(table)
    
    if not processed_dfs:
        print("No data loaded from any table after processing.")
        return

    # Merge all processed DataFrames on 'code_site' and 'date_de_debut' (outer join to keep nulls).
    merged_df = merge_dataframes(processed_dfs)
    
    # Replace null numeric values with the column mean.
    merged_df = replace_null_values_by_mean(merged_df)
    
    # Now perform unit conversion.
    merged_df = convert_units(merged_df)
    
    # Aggregate pollutant values.
    merged_df = aggregate_valeurs(merged_df)

    # Shift and calculate the difference between the current values and the values from 6 hours ago.
    merged_df = shift_and_calculate_diff_6_hours_ago(merged_df)

    # Calculate the percentage change in particle values between the current values and the values from 6 hours ago.
    merged_df = calculate_particle_variation(merged_df)
    
    print("Curated DataFrame:")
    print(merged_df.head(10))
    print("Shape of the final DataFrame:", merged_df.shape)

    # Load PostgreSQL configuration
    pg_config = config["postgresql"]
    print("PostgreSQL configuration:", pg_config)
    
    # Ingest the final DataFrame into PostgreSQL (table named "curated").
    ingest_dataframe_to_postgres(merged_df, "curated", pg_config)

    elapsed_time = time.time() - start_time
    print(f"Total time taken: {int(elapsed_time // 60)} minutes and {elapsed_time % 60:.2f} seconds.")

    return elapsed_time

if __name__ == "__main__":
    main()
    # elapsed_times = []
    # for i in range(5):
    #     elapsed_times.append(main())
    # print(elapsed_times)
    # print("Average time taken:", sum(elapsed_times) / len(elapsed_times))
