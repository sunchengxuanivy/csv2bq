import re
import sys

from google.cloud import storage, bigquery

project_name = sys.argv[1]
dataset_id = sys.argv[2]
table_id = sys.argv[3]
source_data_bucket = sys.argv[4]
intermediate_dir = sys.argv[5]

storage_client = storage.Client()
bucket = storage_client.get_bucket(source_data_bucket)
objects = bucket.list_blobs(prefix=intermediate_dir)
urls = []
for blob in objects:
    if re.match('.*\.csv', blob.name):
        urls.append('gs://{bucket}/{object}'.format(bucket=source_data_bucket, object=blob.name))

client = bigquery.Client(project=project_name)
job_config = bigquery.LoadJobConfig()
job_config.skip_leading_rows = 1
job_config.quote_character = '\"'
job_config.field_delimiter = ','
job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
job_config.source_format = bigquery.SourceFormat.CSV
table = '{}.{}.{}'.format(project_name, dataset_id, table_id)

load_job = client.load_table_from_uri(
    urls, table,
    job_config=job_config)

load_job.result()  # Waits for table load to complete.
