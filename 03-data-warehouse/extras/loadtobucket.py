import io
import os
import requests
import pandas as pd
from google.cloud import storage


os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'terrademo/keys/my_creds.json'

def download_files(filename):
    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-{}.parquet"
    for i in range(1, 13):
        os.system(
            f"wget {url.format(str(i).zfill(2))} -O {filename.format(str(i).zfill(2))}")


def upload_blob(bucket_name, file_prefix, num_files):
    """Uploads files to the bucket."""
    # Initialize a client
    storage_client = storage.Client()
    print(storage_client.project)

    # Get the bucket
    bucket = storage_client.bucket(bucket_name)
    print(bucket)

    for i in range(1, num_files + 1):
        source_file_name = f'03-data-warehouse/extras/files/{file_prefix}{i:02d}.parquet'
        destination_blob_name = f'2022/{file_prefix}{i:02d}.parquet'

        # Define the destination blob
        blob = bucket.blob(destination_blob_name)

        # Upload the file
        blob.upload_from_filename(source_file_name)

        print(f'File {source_file_name} uploaded to {destination_blob_name}.')


# Replace these with your own values
bucket_name = 'daniel-demage'
file_prefix = 'green_tripdata_2022-'
num_files = 12

upload_blob(bucket_name, file_prefix, num_files)
