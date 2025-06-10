from google.cloud import bigquery, secretmanager
import requests
import pandas as pd
import time
import sys
import random
from datetime import datetime, timedelta
import pytz

PROJECT_ID = 'your_project_id_here'

# Set up BigQuery client
def initialize_bq_client():
    return bigquery.Client.from_service_account_json("path_to_your_json_file")

# Set up Secret Manager client
def initialize_secret_manager_client():
    return secretmanager.SecretManagerServiceClient()

# Get secrets from Secret Manager
def access_secret_version(secret_id):
    name = f"projects/{PROJECT_ID}/secrets/{secret_id}/versions/latest"
    response = sm_client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")

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

# Wrapper to handle BQ upload
def upload_to_bigquery(project_id, dataset_id, table_id, data):
    save_data_to_bq_in_batches(project_id, dataset_id, table_id, data)

# Get token from Twitch
def get_oauth_token(client_id, client_secret):
    response = requests.post("https://id.twitch.tv/oauth2/token", params={
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "client_credentials"
    })
    data = response.json()
    return data.get("access_token"), data.get("expires_in")

# Only refresh if token expires soon
def check_token_expiry(expiration_time, threshold_hours=48):
    return expiration_time - datetime.now() < timedelta(hours=threshold_hours)

# Basic exponential backoff
def exponential_backoff(retry_count):
    return min(((2 ** retry_count) + random.randint(0, 1000)) / 1000, 60)

# Get stream data from Twitch API
def get_twitch_streams(params, new_columns):
    global twitch_client_id, twitch_oauth_token

    retry_count = 0
    streams_df = pd.DataFrame(columns=new_columns)
    url = "https://api.twitch.tv/helix/streams"
    headers = {
        "Client-ID": twitch_client_id,
        "Authorization": f"Bearer {twitch_oauth_token}"
    }

    while True:
        try:
            response = requests.get(url, headers=headers, params=params, timeout=10)
            if int(response.headers.get('ratelimit-remaining', 0)) == 0:
                wait_time = max(0, int(response.headers.get('ratelimit-reset', time.time() + 60)) - time.time())
                print(f"Rate limit reached. Waiting {wait_time:.1f} sec.")
                time.sleep(wait_time)
                continue
        except requests.RequestException as e:
            if retry_count < 5:
                delay = exponential_backoff(retry_count)
                print(f"Retry {retry_count+1}: waiting {delay:.2f} sec due to {e}")
                time.sleep(delay)
                retry_count += 1
                continue
            print("Max retries exceeded.")
            return None

        if response.status_code != 200:
            print(f"Request failed: {response.status_code} - {response.text}")
            return None

        try:
            data = response.json()
            if "data" in data:
                df = pd.json_normalize(data['data']).reindex(columns=new_columns)
                streams_df = pd.concat([streams_df, df], ignore_index=True)
                if "pagination" in data and "cursor" in data["pagination"]:
                    params["after"] = data["pagination"]["cursor"]
                else:
                    break
            else:
                break
        except Exception as e:
            print(f"Data parsing error: {e}")
            break

    return streams_df

# Define BQ table schema
schema = [
    bigquery.SchemaField('id', 'INTEGER'),
    bigquery.SchemaField('user_id', 'INTEGER'),
    bigquery.SchemaField('user_login', 'STRING'),
    bigquery.SchemaField('user_name', 'STRING'),
    bigquery.SchemaField('game_id', 'INTEGER'),
    bigquery.SchemaField('game_name', 'STRING'),
    bigquery.SchemaField('type', 'STRING'),
    bigquery.SchemaField('title', 'STRING'),
    bigquery.SchemaField('tags', 'STRING'),
    bigquery.SchemaField('viewer_count', 'INTEGER'),
    bigquery.SchemaField('started_at', 'TIMESTAMP'),
    bigquery.SchemaField('language', 'STRING'),
    bigquery.SchemaField('thumbnail_url', 'STRING'),
    bigquery.SchemaField('tag_ids', 'STRING'),
    bigquery.SchemaField('is_mature', 'BOOLEAN'),
    bigquery.SchemaField('last_updated', 'TIMESTAMP')
]

# Set up clients + get Twitch credentials
bq_client = initialize_bq_client()
sm_client = initialize_secret_manager_client()
twitch_client_id = access_secret_version('your_secret_name')
twitch_client_secret = access_secret_version('your_secret_name_2')

# First token fetch
twitch_oauth_token, expires_in = get_oauth_token(twitch_client_id, twitch_client_secret)
expiration_time = datetime.now() + timedelta(seconds=expires_in)

# Keep script running to pull data repeatedly
while True:
    start_time = time.time()

    if check_token_expiry(expiration_time):
        twitch_oauth_token, expires_in = get_oauth_token(twitch_client_id, twitch_client_secret)
        expiration_time = datetime.now() + timedelta(seconds=expires_in)

    columns = ["id", "user_id", "user_login", "user_name", "game_id", "game_name", "type",
               "title", "tags", "viewer_count", "started_at", "language",
               "thumbnail_url", "tag_ids", "is_mature"]

    params = {"type": "all", "first": 100}
    df = get_twitch_streams(params, columns)

    if df is not None and not df.empty:
        df['tags'] = df['tags'].astype(str)
        df['tag_ids'] = df['tag_ids'].astype(str)
        df['game_id'] = pd.to_numeric(df['game_id'], errors='coerce').fillna(0).astype(int)

        current_time_utc = datetime.utcnow().replace(tzinfo=pytz.utc)
        df['last_updated'] = current_time_utc

        try:
            client = initialize_bq_client()
            upload_to_bigquery(PROJECT_ID, "twitch_temp", "livestreams", df)
            print(f"Exported {len(df)} rows at {current_time_utc.isoformat()}")
        except Exception as e:
            print(f"Failed to upload data: {e}")
    else:
        print("No data retrieved this cycle.")

    print(f"Iteration time: {(time.time() - start_time)/60:.2f} minutes")
