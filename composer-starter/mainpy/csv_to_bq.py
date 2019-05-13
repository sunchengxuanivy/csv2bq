import re

from google.cloud import storage, bigquery

project_name = 'fifty-shades-of-brown'
dataset_id = 'dominatrix'
table_id = 'loan_dominatrix'

storage_client = storage.Client()
bucket = storage_client.get_bucket('src_raw-data_bucket')
objects = bucket.list_blobs(prefix='loan-1')
urls = []
for blob in objects:
    if re.match('.*\.csv', blob.name):
        urls.append('gs://{bucket}/{object}'.format(bucket='src_raw-data_bucket', object=blob.name))

client = bigquery.Client(project='fifty-shades-of-brown')
job_config = bigquery.LoadJobConfig()
job_config.skip_leading_rows = 1
job_config.quote_character = '\"'
job_config.field_delimiter = ','
job_config.source_format = bigquery.SourceFormat.CSV
table = '{}.{}.{}'.format(project_name, dataset_id, table_id)

load_job = client.load_table_from_uri(
    urls, table,
    job_config=job_config)

load_job.result()  # Waits for table load to complete.
