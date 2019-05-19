import sys
import argparse
from subprocess import PIPE, Popen
from google.cloud import bigquery


def run_bq_query(bq_sql,
                 target_project_id,
                 target_dataset,
                 target_table,
                 table_overwrite=True,
                 use_legacy_sql=False,
                 export_to_csv=False,
                 csv_overwrite=True,
                 export_gcs_path=None):
    client = bigquery.Client(target_project_id)
    dataset_ref = client.create_dataset(target_dataset, exists_ok=True)
    table_ref = dataset_ref.table(target_table)
    job_config = bigquery.QueryJobConfig()

    if table_overwrite:
        job_config.write_disposition = bigquery.job.WriteDisposition.WRITE_TRUNCATE
    else:
        job_config.write_disposition = bigquery.job.WriteDisposition.WRITE_APPEND

    job_config.destination = table_ref
    job_config.use_legacy_sql = use_legacy_sql
    load_job = client.query(
        query=bq_sql,
        job_config=job_config)

    load_job.result()  # Waits for table load to complete.

    if export_to_csv:
        if csv_overwrite:
            rmhdfs = Popen(["hadoop", "fs", "-rm", export_gcs_path], stdin=PIPE, bufsize=-1)
            rmhdfs.communicate()
        extract_job = client.extract_table(source=table_ref, destination_uris=[export_gcs_path])
        extract_job.result()


parser = argparse.ArgumentParser(description='Run a BigQuery query and save result in a BigQuery table')
parser.add_argument('--sql', '-s', dest='sql', required=True)
parser.add_argument('--target-project-id', '-p', dest='target_project_id', required=True)
parser.add_argument('--target-dataset', '-d', dest='target_dataset', required=True)
parser.add_argument('--target-table', '-t', dest='target_table', required=True)
parser.add_argument('--table-overwrite', '-to', dest='table_overwrite', required=False, action='store_true')
parser.add_argument('--csv-overwrite', '-co', dest='csv_overwrite', required=False, action='store_true')
parser.add_argument('--use-legacy-sql', '-u', dest='use_legacy_sql', required=False, action='store_true')
parser.add_argument('--export-to-csv', '-e', dest='export_to_csv', required=False, action='store_true')
parser.add_argument('--export-gcs-path', '-gp', dest='export_gcs_path', required=False)

argument = parser.parse_args(sys.argv[1:])
run_bq_query(bq_sql=argument.sql,
             target_project_id=argument.target_project_id,
             target_dataset=argument.target_dataset,
             target_table=argument.target_table,
             table_overwrite=argument.table_overwrite,
             use_legacy_sql=argument.use_legacy_sql,
             export_to_csv=argument.export_to_csv,
             csv_overwrite=argument.csv_overwrite,
             export_gcs_path=argument.export_gcs_path)
