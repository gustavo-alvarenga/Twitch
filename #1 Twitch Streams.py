from google.cloud import bigquery, secretmanager
import requests
import pandas as pd
import time
import sys
import random
from datetime import datetime, timedelta
import pytz


# Define Project ID
PROJECT_ID = 'your_project_id_here'

# Initialize BigQuery client
def initialize_bq_client():
    # Set up the credentials with the key ID
    bq_client = bigquery.Client.from_service_account_json("path_to_your_json_file")
    return bq_client

# Initialize Secret Manager client
def initialize_secret_manager_client():
    global sm_client
    sm_client = secretmanager.SecretManagerServiceClient()
    return sm_client

# Function to access secret from Secret Manager
def access_secret_version(secret_id):
    name = f"projects/{PROJECT_ID}/secrets/{secret_id}/versions/latest"
    response = sm_client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")



# Function to create table in BigQuery
def create_table(current_project_id, current_dataset_id, current_table_id):

    # Create the table
    table_ref = client.dataset(current_dataset_id).table(current_table_id)
    table = bigquery.Table(table_ref, schema=schema)
    table = client.create_table(table)

# Function to save data in a BigQuery table
def save_data_to_bq_in_batches(project_id, dataset_id, table_id, data, batch_size=10000):

    # Initialize a list to store errors, if any
    all_errors = []

    # Calculate the number of batches needed
    num_batches = (len(data) + batch_size - 1) // batch_size

    # Create a reference to the table
    table_ref = client.dataset(dataset_id, project=project_id).table(table_id)

    # Process data in batches
    for i in range(num_batches):
        start_idx = i * batch_size
        end_idx = (i + 1) * batch_size
        batch_data = data.iloc[start_idx:end_idx].where(pd.notna, None).to_dict(orient='records')

        try:
            # Insert the batch into the table
            errors = client.insert_rows(table_ref, batch_data, selected_fields=schema)

        except requests.exceptions.HTTPError as e:

            if err.response.status_code == 404:

                try:
                    time.sleep(5)
                    errors = client.insert_rows(table_ref, batch_data, selected_fields=schema)

                except:
                    print("Table not found error:", e)
                    print("Retrying the operation")
                    return save_data_to_bq_in_batches(table_id, data)

        if errors:
            all_errors.extend(errors)

    if not all_errors:
        pass
    else:
        print(f'Errors encountered: {all_errors}') 

# Function that checks if table already exists
def upload_to_bigquery(project_id, dataset_id, table_id, data):

    save_data_to_bq_in_batches(project_id, dataset_id, table_id, data)

    
# Function to get OAuth token from Twitch
def get_oauth_token(client_id, client_secret):
    
    import requests
    from datetime import datetime, timedelta
    token_url = "https://id.twitch.tv/oauth2/token"
    params = {
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "client_credentials"
    }
    response = requests.post(token_url, params=params)
    data = response.json()
    return data.get("access_token"), data.get("expires_in")

# Function to check OAuth token expiration datetime from Twitch
def check_token_expiry(expiration_time, threshold_hours=48):
    
    import requests
    from datetime import datetime, timedelta

    remaining_time = expiration_time - datetime.now()

    # Update the token only if there are less than threshold_hours remaining
    if remaining_time < timedelta(hours=threshold_hours):
        print("Updating Token")
        sys.stdout.flush()
        return True

    return False        
     
# Exponential Backoff
def exponential_backoff(retry_count):
    max_backoff = 60  # Maximum delay in seconds
    backoff_time = min(((2 ** retry_count) + random.randint(0, 1000)) / 1000, max_backoff)
    return backoff_time


# Function to retrieve streams from Twitch
def get_twitch_streams(params, new_columns):

    # Initialize global variables
    global twitch_client_id
    global twitch_oauth_token

    # Initialize local variables
    retry_count = 0
    streams_df = pd.DataFrame(columns=new_columns)

    url = "https://api.twitch.tv/helix/streams"
    headers = {
        "Client-ID": twitch_client_id,
        "Authorization": f"Bearer {twitch_oauth_token}"
    }

    streams = []

    while True:

        try:

            response = requests.get(url, headers=headers, params=params, timeout=10)
            rate_limit_remaining = int(response.headers.get('ratelimit-remaining', 0))
            rate_limit_limit = int(response.headers.get('ratelimit-limit', 30))
            
            # Check if rate was exceeded
            if rate_limit_remaining == 0:
                reset_time = int(response.headers.get('ratelimit-reset', time.time() + 60))
                wait_time = max(0, reset_time - time.time())
                print(f"Rate limit reached. Waiting for {wait_time} seconds...")
                sys.stdout.flush()
                time.sleep(wait_time)
                continue

        except requests.ConnectionError as e:
            print("Error during network request:", e)
            sys.stdout.flush()
            
            # Limit the number of retries by 5
            if retry_count < 5:  
                delay = exponential_backoff(retry_count)
                print(f"Retrying in {delay} seconds")
                sys.stdout.flush()
                time.sleep(delay)
                return get_twitch_streams(params)
            else:
                print("Maximum retry count exceeded. Exiting.")
                sys.stdout.flush()
                return None

        except requests.HTTPError as e:
            if response.status_code == 401 and "Unauthorized" in response.text:
                print("Token expired. Refreshing token...")
                sys.stdout.flush()
                twitch_oauth_token, expires_in = get_oauth_token(twitch_client_id, twitch_client_secret)
                expiration_time = datetime.now() + timedelta(seconds=expires_in)                   
                return get_twitch_streams(params)

            print("Error during network request:", e)   
            sys.stdout.flush()
            return get_twitch_streams(params)

        except requests.Timeout:
            print("Request timed out.")
            sys.stdout.flush()
            if retry_count < 5:  # Limit the number of retries
                delay = exponential_backoff(retry_count)
                print(f"Retrying in {delay} seconds...")
                sys.stdout.flush()
                time.sleep(delay)
                return get_twitch_streams(params)
            else:
                print("Maximum retry count exceeded. Exiting.")
                sys.stdout.flush()
                return None

        except Exception as e:
            print("Unexpected error:", e)  
            sys.stdout.flush()


        if response.status_code != 200:
            print("Request unsuccessful")
            print("Response content:", response.content)
            sys.stdout.flush()
            return None
        
        

        # Process retrieved data, if successful
        try:
            data = response.json()
            streams_df_current = pd.json_normalize(data['data'])


        except Exception as e:
            print(f"An exception occurred when processing Twitch data: {e}")
            sys.stdout.flush()
            break

        if "data" in data:

            streams_df_current = streams_df_current.reindex(columns=new_columns)

            # Concatenate dfs
            streams_df = pd.concat([streams_df_current, streams_df], ignore_index=True)

            # Paginate
            if "pagination" in data and "cursor" in data["pagination"]:
                params["after"] = data["pagination"]["cursor"]

            else:
                break
        else:
            print("Error retrieving data from Twitch API:")
            sys.stdout.flush()
            break

    return streams_df

# Schema for BigQuery table
schema = [
    bigquery.SchemaField('id', 'INTEGER', mode='NULLABLE'),
    bigquery.SchemaField('user_id', 'INTEGER', mode='NULLABLE'),
    bigquery.SchemaField('user_login', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('user_name', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('game_id', 'INTEGER', mode='NULLABLE'),
    bigquery.SchemaField('game_name', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('type', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('title', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('tags', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('viewer_count', 'INTEGER', mode='NULLABLE'),
    bigquery.SchemaField('started_at', 'TIMESTAMP', mode='NULLABLE'),
    bigquery.SchemaField('language', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('thumbnail_url', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('tag_ids', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('is_mature', 'BOOLEAN', mode='NULLABLE'),
    bigquery.SchemaField('last_updated', 'TIMESTAMP', mode='NULLABLE')   
]


# Initialize clients
global bq_client
global sm_client
bq_client = initialize_bq_client()
sm_client = initialize_secret_manager_client()

# Define and retrieve twitch info
global twitch_client_id
global twitch_client_secret
twitch_client_id = access_secret_version('your_secret_name')
twitch_client_secret = access_secret_version('your_secret_name_2')


# Initial token retrieval from Twitch
global twitch_oauth_token
twitch_oauth_token, expires_in = get_oauth_token(twitch_client_id, twitch_client_secret)
expiration_time = datetime.now() + timedelta(seconds=expires_in)

print(f"Client oauth_token: {twitch_oauth_token}")
print(f'Client oauth_token expiration datetime: {expiration_time}')
sys.stdout.flush()

# While to make sure the code runs indefinitely
while True:
    
    # Start the iteration timer
    start_time = time.time()

    # Check token expiration and only updates if the function returns true (48h prior to the expiration datetime)
    if check_token_expiry(expiration_time):
        
        # Update the token
        twitch_oauth_token, expires_in = get_oauth_token(twitch_client_id, twitch_client_secret)
        expiration_time = datetime.now() + timedelta(seconds=expires_in)
        print(f"Client oauth_token: {twitch_oauth_token}")
        print(f'Client oauth_token expiration datetime: {expiration_time}')
        sys.stdout.flush()

        
    # Define columns for df
    columns = ["id","user_id","user_login","user_name",'game_id', 'game_name', 'type',"title","tags","viewer_count",
                "started_at","language","thumbnail_url","tag_ids","is_mature"]

    # Set the variable
    elapsed_game_seconds = 0

    # Get data from twitch and return the max amount of results per page
    params = {"type": "all", "first": 100}

    df = pd.DataFrame(columns=columns)
    df = get_twitch_streams(params, columns)


    if df is not None and not df.empty:

        # Convert tags and tag_ids to string
        df['tags'] = df['tags'].astype(str)
        df['tag_ids'] = df['tag_ids'].astype(str)

        # Transform all empty game_id in 0s
        df['game_id'] = pd.to_numeric(df['game_id'], errors='coerce').fillna(0).astype(int)

        # Get current UTC time and format it
        current_time_utc = datetime.utcnow()

        # Set UTC timezone
        utc_timezone = pytz.timezone('UTC')
        current_time_utc = utc_timezone.localize(current_time_utc)

        # Convert to Eastern Standard Time (EST)
        est_timezone = pytz.timezone('US/Eastern')
        current_time_est = current_time_utc.astimezone(est_timezone)
        current_time_est_str = current_time_est.strftime('%Y-%m-%dT%H:%M:%S EST')

        # Add a new column with the formatted current UTC time
        df['last_updated'] = current_time_utc


        # Save data to BigQuery
        try:

            # Initializing client
            client = initialize_bq_client()

            # Upload data to BigQuery
            upload_to_bigquery(PROJECT_ID, "twitch_temp", "livestreams", df)

            print(f"Data exported for game successfully! {len(df)} rows exported at {current_time_est_str}")
            sys.stdout.flush()
            del df

        except Exception as e:
            print(f'Error while preparing to save data to BigQuery: {e}')
            sys.stdout.flush()

    else:
        print("No data to export")
        sys.stdout.flush()
        
    del params

    # Calculate the elapsed time
    end_time = time.time()
    iteration_elapsed_time = end_time - start_time
    print(f"Iteration completed in {iteration_elapsed_time / 60:.2f} minutes")
    sys.stdout.flush()
