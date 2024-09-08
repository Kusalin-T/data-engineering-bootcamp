import json

from google.cloud import storage
from google.oauth2 import service_account


DATA_FOLDER = "data"
BUSINESS_DOMAIN = "greenery"
project_id = "skooldio-deb4-project"
location = "asia-southeast1"
bucket_name = "deb4-bootcamp-004"
data = "addresses"

# Prepare and Load Credentials to Connect to GCP Services
keyfile_gcs = "gcs-deb4-day3-key.json"
service_account_info_gcs = json.load(open(keyfile_gcs))
credentials_gcs = service_account.Credentials.from_service_account_info(
    service_account_info_gcs
)

# Load data from Local to GCS
storage_client = storage.Client(
    project=project_id,
    credentials=credentials_gcs,
)
bucket = storage_client.bucket(bucket_name)

# file_path = f"{DATA_FOLDER}/{data}.csv"
# destination_blob_name = f"raw/{BUSINESS_DOMAIN}/{data}/{data}.csv"

# # YOUR CODE HERE TO LOAD DATA TO GCS
# blob = bucket.blob(destination_blob_name)
# blob.upload_from_filename(file_path)

data_filename_list=[
    'addresses',
    'events',
    'order-items',
    'orders',
    'products',
    'promos',
    'users'
]

for data_filename in data_filename_list:
    file_path = f"{DATA_FOLDER}/{data_filename}.csv"
    destination_blob_name = f"raw/{BUSINESS_DOMAIN}/{data_filename}/{data_filename}.csv"

    # YOUR CODE HERE TO LOAD DATA TO GCS
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(file_path)