import csv
import json
import random
from azure.storage.blob import BlobServiceClient
import os
import logging

def read_position_index(pos_path, default_start=1):
    environment = os.environ.get('ENVIRONMENT')
    if environment == 'LOCAL':
        # Try to read the last position from the file
        try:
            with open(pos_path, 'r') as f:
                start_row = int(f.read().strip())
                # Skip header row
                if start_row == 0:
                    start_row = 1
        except (FileNotFoundError, ValueError):
            # If file not found or empty, start from default_start
            start_row = default_start

        return start_row
    else:
        connection_string = os.environ.get('az_blob_stor_cnx_str')
        blob_service_client = BlobServiceClient.from_connection_string(connect_str)
        container_name = 'gazacontainer'
        blob_name = pos_path
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        downloader = blob_client.download_blob(max_concurrency=1, encoding='UTF-8')
        index_text = downloader.readall()
        start_row = int(index_text)
        logging.info("Successfully read data from blob: %d", start_row)
        return start_row

def write_position_index(new_start_row, pos_path):
    environment = os.environ.get('ENVIRONMENT')
    if environment == 'LOCAL':
        with open(pos_path, 'w') as f:
            f.write(str(new_start_row))
    else:
        connection_string = os.environ.get('az_blob_stor_cnx_str')
        blob_service_client = BlobServiceClient.from_connection_string(connect_str)
        container_name = 'gazacontainer'
        blob_name = pos_path
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        blob_client.upload_blob(new_start_row, overwrite=True)
        logging.info("Successfully wrote data to blob: %s", blob_name)

def read_and_continue(csv_path, pos_path, num_rows=3):

    rows_read = 0
    rows = []

    start_row = read_position_index(pos_path)

    # Open the CSV and read specified rows
    with open(csv_path, 'r') as csvfile:
        reader = csv.reader(csvfile)
        for i, row in enumerate(reader):
            if i >= start_row:
                rows.append(row)
                rows_read += 1
                if rows_read == num_rows:
                    break
    
    # We are at the end of file
    if len(rows) < num_rows:
        new_start_row = 1
    else:
        # Update the start row for the next read
        new_start_row = start_row + rows_read

    write_position_index(new_start_row, pos_path)
    
    # Dictionary keys
    keys = ['Gender', 'Name', 'Age']

    # Transform each sublist into a dictionary
    result_dicts = [dict(zip(keys, row)) for row in rows]

    return result_dicts, new_start_row

def load_tweets_content(json_file):
    with open(json_file) as tweet_settings_file:
        tweet_settings_content = tweet_settings_file.read()
    
    parsed_json = json.loads(tweet_settings_content)

    return parsed_json

def tweet_text_generator(csv_file, position_file, json_file):
    rows, new_start_row = read_and_continue(csv_file, position_file)
    tweets_content = load_tweets_content(json_file)

    tweet = f"{random.choice(tweets_content['header'])}\n\n"

    for row in rows:
        tweet += f"{row['Name']} ({row['Gender']}) - Age: {row['Age']}\n"
    
    tweet += f"\n{random.choice(tweets_content['footer'])}\n\n"
    tweet += f"{random.choice(tweets_content['conclusion'])}\n"
    random_sample_tags = random.sample(tweets_content["tags"], 2)
    tweet += " ".join(random_sample_tags)

    return tweet, new_start_row

#Usage
if __name__ == "__main__":
    csv_file = 'martyrs.csv'
    position_file = 'last_position.txt' # File to store the last read row number

    json_file = 'tweets_content.json'

    tweet, new_start_row = tweet_text_generator(csv_file, position_file, json_file)
    print(tweet)