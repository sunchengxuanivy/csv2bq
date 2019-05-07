# csv2bq
For data ingestion part of POC


1. Please create an Environment, please place flow.py under the DAGs bucket.
2. Please place hello_bigquery.py under bucket gs://src_raw-data_bucket/, since it's hard coded in flow.py(line 48).
3. Go back to Environment Airflow webserver. the DAG will automatically run. and you may manually trigger it on console.