import sys

from google.cloud import storage

source_data_bucket = sys.argv[1]
intermediate_dir = sys.argv[2]

storage_client = storage.Client()
bucket = storage_client.get_bucket(source_data_bucket)
blobs = bucket.list_blobs(prefix=intermediate_dir)
for blob in blobs:
    blob.delete()
