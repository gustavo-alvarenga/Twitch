import pandas as pd
from google.cloud import bigquery
import time
import math
import sys
import requests
from datetime import datetime, timedelta

PROJECT_ID = 'your_project_id_here'

# Set up BigQuery client
def initialize_bq_client():
    # Set up the credentials with the key ID
    bq_client = bigquery.Client.from_service_account_json("path_to_your_json_file")
    return bq_client

# List partitions
def list_partitions(project_id, dataset_id, table_id):
    query = f"""
    SELECT DISTINCT DATE_TRUNC(started_at, HOUR) as partition_time
    FROM `{project_id}.{dataset_id}.{table_id}`
    ORDER BY partition_time ASC
    """
    query_job = client.query(query)
    partitions = query_job.result()
    return [row.partition_time for row in partitions]

# Process data that was ingested before the start_hour
def process_bigquery_data(project_id, dataset_id, table_id):

    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    batch_size = 100000
    total_rows_processed = 0
    total_rows = 0

    all_dfs = pd.DataFrame()

    # List all partitions
    partitions = list_partitions(project_id, dataset_id, table_id)

    for partition in partitions:
        
        start_time = time.time()

        print(f"Starting with partition {partition}")
        sys.stdout.flush()            
        
        start_offset = 0
        minutes_taken = 0
        partition_time_taken = 0
        
        # Read the data in batches so not to exceeed BigQuery's limit or not to overload the virtual machine
        while True:

            query_template = f"""
            SELECT * FROM `{table_ref}` 
            WHERE TIMESTAMP_TRUNC(started_at, HOUR) = TIMESTAMP('{partition}')
            AND last_updated < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 MINUTE)
            ORDER BY user_id ASC, last_updated DESC
            LIMIT {batch_size} OFFSET {start_offset}
            """

            try:
                query_job = client.query(query_template)
                results = query_job.result()
            except Exception as e:
                print(f"Failed to retrieve data from {table_ref}: {e}")
                sys.stdout.flush()
                return    

            rows = list(results)
            row_count = len(rows)
            
            
            # Break if there are no results
            if row_count == 0:
                
                # Calculate the elapsed time
                end_time = time.time()
                iteration_elapsed_time = end_time - start_time
                minutes_taken = iteration_elapsed_time / 60
                print(f"All rows have been successfully processed in {minutes_taken:.2f} minutes")
                sys.stdout.flush()                    
                break

            df = pd.DataFrame([dict(row) for row in rows])

            # Call function to process the data
            processed_df = consolidating_game_stats(df)
            del df
            
            processed_df['last_processed'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            # Call function to upload the data
            save_data_to_bq_in_batches("your_project_id_here", "twitch", "final_table", processed_df, schema_output)
            total_rows_processed += len(processed_df)

            total_rows += row_count
            start_offset += batch_size

        print(f"Total data retrieved from temp database so far: {total_rows:,} rows")
        sys.stdout.flush()

        print(f"Data processed and uploaded so far: {total_rows_processed} rows")
        sys.stdout.flush()
        
        # Delete partition
        delete_template = f"""
        DELETE FROM `{table_ref}` 
        WHERE TIMESTAMP_TRUNC(started_at, HOUR) = TIMESTAMP('{partition}')
        AND last_updated < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {int(minutes_taken)} MINUTE)
        """
        
        try:
            query_job = client.query(delete_template)
            results = query_job.result()

            partition_time_taken += minutes_taken
            
            print(f"Partition successfully deleted. Total time: {partition_time_taken:.2f} minutes")
            sys.stdout.flush()
            
        except Exception as e:
            print(f"Failed to delete data from {table_ref}: {e}")
            sys.stdout.flush()
            break    
 

def consolidating_game_stats(df, time_interval_min=10):
    
    # Drop duplicates, sort df, and reset index 
    df = df.drop_duplicates()
    df = df.sort_values(by=['id','last_updated'], ascending=[False, False], ignore_index=True)
    df.reset_index(drop=True, inplace=True)
    
    # Define lists
    duration_list = []
    watchtime_list = []
    
    # Calculate the duration between last_updated and started_at for rows with different ids
    df['duration'] = (df['last_updated'] - df['started_at']).dt.total_seconds()
    df['viewer_count'] = pd.to_numeric(df['viewer_count'], errors='coerce')
    
    # Iterate through the DataFrame
    for i in range(len(df)-1):
        if df['id'].iloc[i] == df['id'].iloc[i+1]:
            duration = (df['last_updated'].iloc[i] - df['last_updated'].iloc[i+1]).total_seconds()
        else:
            # duration = df['duration'].iloc[i]
            duration = 0
        duration = int(duration) if not math.isnan(duration) else 0
        duration_list.append(duration)
        watchtime_list.append(int(duration*df['viewer_count'].iloc[i]))
    
    # Calculate the duration for the last row (since there's no "next" row)
    if not df.empty and 'duration' in df.columns and len(df) > 0:
        # duration = df['duration'].iloc[-1]
        # duration = int(duration) if not math.isnan(duration) else 0
        duration = 0
        duration_list.append(duration)
        watchtime_list.append(int(duration*df['viewer_count'].iloc[-1]))
    
    df["duration_in_seconds"] = duration_list
    df["watchtime_in_seconds"] = watchtime_list
    del duration_list, watchtime_list

    time_interval_min_string = f"{time_interval_min}min"
    df['last_updated_xmin'] = df['last_updated'].dt.floor(time_interval_min_string)
    
    result = df.groupby(['game_id', 'language', 'last_updated_xmin']).agg({
        'game_name': 'last',
        'duration_in_seconds': 'sum',
        'watchtime_in_seconds': 'sum',
        'viewer_count': 'sum',
        'user_id': 'nunique'
    }).reset_index()
    del df
    
    result.reset_index(drop=True, inplace=True)
    result.rename(columns={'viewer_count': 'peak_viewer_count', 'last_updated_xmin': 'last_updated', 'user_id': 'unique_streamers'}, inplace=True)

    # Extract column names from the schema and reorder DataFrame columns  
    expected_order = [field.name for field in schema_output_games]
    result = result[expected_order]

    return result

# Save data to BQ in batches to avoid issues with large inserts
def save_data_to_bq_in_batches(project_id, dataset_id, table_id, data, batch_size=10000):
    all_errors = []
    num_batches = (len(data) + batch_size - 1) // batch_size
    table_ref = client.dataset(dataset_id, project=project_id).table(table_id)

    for i in range(num_batches):
        batch_data = data.iloc[i * batch_size:(i + 1) * batch_size].where(pd.notna, None).to_dict(orient='records')
        try:
            errors = client.insert_rows(table_ref, batch_data, selected_fields=schema)
        except requests.exceptions.HTTPError as e:
            time.sleep(5)
            errors = client.insert_rows(table_ref, batch_data, selected_fields=schema)
        if errors:
            all_errors.extend(errors)

    if all_errors:
        print(f'Errors encountered: {all_errors}')


start_time = time.time()

# Define BQ table schema
schema_output = [
    bigquery.SchemaField('id', 'INTEGER', mode='NULLABLE'),
    bigquery.SchemaField('game_id', 'INTEGER', mode='NULLABLE'),
    bigquery.SchemaField('user_id', 'INTEGER', mode='NULLABLE'),
    bigquery.SchemaField('user_login', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('user_name', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('game_name', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('type', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('title', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('tags', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('started_at', 'TIMESTAMP', mode='NULLABLE'),
    bigquery.SchemaField('last_updated', 'TIMESTAMP', mode='NULLABLE'),
    bigquery.SchemaField('language', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('thumbnail_url', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('tag_ids', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('is_mature', 'BOOLEAN', mode='NULLABLE'),
    bigquery.SchemaField('duration_in_seconds', 'INTEGER', mode='NULLABLE'),
    bigquery.SchemaField('watchtime_in_seconds', 'INTEGER', mode='NULLABLE'),
    bigquery.SchemaField('peak_viewer_count', 'INTEGER', mode='NULLABLE'),
    bigquery.SchemaField('last_processed', 'TIMESTAMP', mode='NULLABLE'),
    ]

# Define BQ table schema
schema_output_games = [
    bigquery.SchemaField('game_id', 'INTEGER', mode='NULLABLE'),
    bigquery.SchemaField('game_name', 'STRING', mode='NULLABLE'),    
    bigquery.SchemaField('language', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('duration_in_seconds', 'INTEGER', mode='NULLABLE'),
    bigquery.SchemaField('watchtime_in_seconds', 'INTEGER', mode='NULLABLE'),
    bigquery.SchemaField('peak_viewer_count', 'INTEGER', mode='NULLABLE'),
    bigquery.SchemaField('last_updated', 'TIMESTAMP', mode='NULLABLE'),
    bigquery.SchemaField('unique_streamers', 'INTEGER', mode='NULLABLE')    
    ]

global client
client = initialize_bq_client()    

df = process_bigquery_data("the-outcome-429113-s9", "twitch_temp", "livestreams")

end_time = time.time()
iteration_elapsed_time = end_time - start_time
print(f"Iteration completed in {iteration_elapsed_time / 60:.2f} minutes")
sys.stdout.flush()
