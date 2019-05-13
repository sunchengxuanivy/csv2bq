from google.cloud import storage

storage_client = storage.Client()
bucket = storage_client.get_bucket('src_raw-data_bucket')
blobs = bucket.list_blobs(prefix='loan-1')
for blob in blobs:
    blob.delete()
