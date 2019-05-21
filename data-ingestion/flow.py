import datetime

from airflow import models
from airflow.contrib.operators import dataproc_operator
from airflow.utils import trigger_rule

"""
    This file defines an airflow DAG to ingest csv data from GCS bucket to Bigquery table.
    This DAG flow is located in source project.(MARK I, MARK II)
    Please place this file in the DAG bucket after Composer is created.  
        --> thus you will see graphic view in the Airflow portal.
    
    ** The DAG will be triggered immediately after the DAG file is placed, because the startdate is set to YESTERDAY.
    
    You may have to do below items to make the Airflow DAG valid:
    1. Create a dedicated service account(dedicated for Dataproc cluster), grand role:
            BigQuery Data Editor
            BigQuery Job User
            Dataproc Worker
            Storage Object Admin
            
    2. create GSC bucket for dataproc
    3. create another GSC bucket for placing python scripts.
    4.* create a source GSC bucket, and place loan.csv inside. 
    5. change values of "service_account", "dataproc_bucket", "main_script_bucket", "source_data_bucket" to what you
    have just created in /data-ingestion/variables.json.
    6. change other values if needed in the json file, eg, gcp_project_id, etc.
    7. import json file to Composer: log into Composer's Airflow portal --> Admin --> Variables, browse variable.json 
    and import.
    8. Upload all scripts in /data-ingestion/mainpy directly under bucket created in STEP 3.
    
    Refresh Airflow portal and DAG is good to run. You may manually trigger and try out.

"""
yesterday = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())

default_dag_args = {
    # Setting start date as yesterday starts the DAG immediately when it is
    # detected in the Cloud Storage bucket.
    'start_date': yesterday,
    # To email on failure or retry set 'email' arg to your email and enable
    # emailing here.
    'email_on_failure': False,
    'email_on_retry': False,
    # If a task fails, retry it once after waiting at least 5 minutes
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'project_id': models.Variable.get('gcp_project_id')
}

cluster_name = 'bucket2bq-cluster-{{ ds_nodash }}'
main_bucket = models.Variable.get('main_script_bucket')
source_data_bucket = models.Variable.get('source_data_bucket')
gcp_project_id = models.Variable.get('gcp_project_id')
dataset_id = models.Variable.get('dataset_id')
table_id = models.Variable.get('table_id')
source_data = models.Variable.get('source_data')
intermediate_dir = models.Variable.get('intermediate_dir')

with models.DAG(
        'csv2bq',
        default_args=default_dag_args) as dag:
    # Create a Cloud Dataproc cluster.
    create_dataproc_cluster = dataproc_operator.DataprocClusterCreateOperator(
        task_id='create_dataproc_cluster',
        storage_bucket=models.Variable.get('dataproc_bucket'),
        cluster_name=cluster_name,
        init_actions_uris=['gs://dataproc-initialization-actions/python/pip-install.sh'],
        metadata={'PIP_PACKAGES': 'google-cloud-bigquery==1.11.2 google-cloud-storage==1.15.0'},
        image_version='1.4-debian9',
        num_workers=2,
        service_account=models.Variable.get('service_account'),
        zone=models.Variable.get('dataproc_zone'),
        master_machine_type='n1-standard-1',
        worker_machine_type='n1-standard-1')

    create_bq_tables = dataproc_operator.DataProcPySparkOperator(
        cluster_name=cluster_name,
        task_id='create_bq_tables',
        main='gs://{}/create_table.py'.format(main_bucket),
        arguments=[gcp_project_id, dataset_id, table_id]
    )

    file_etl = dataproc_operator.DataProcPySparkOperator(
        cluster_name=cluster_name,
        task_id='simple-file-etl',
        main='gs://{}/file_etl.py'.format(main_bucket),
        arguments=[source_data_bucket, source_data, intermediate_dir]
    )

    dump_data = dataproc_operator.DataProcPySparkOperator(
        cluster_name=cluster_name,
        task_id='csv-to-bigquery',
        main='gs://{}/csv_to_bq.py'.format(main_bucket),
        arguments=[gcp_project_id, dataset_id,
                   table_id, source_data_bucket, intermediate_dir]
    )

    delete_inter_datafiles = dataproc_operator.DataProcPySparkOperator(
        cluster_name=cluster_name,
        task_id='delete-inter-datafiles',
        main='gs://{}/rm_bucket_folder.py'.format(main_bucket),
        arguments=[source_data_bucket, intermediate_dir]
    )

    # Delete Cloud Dataproc cluster.
    delete_dataproc_cluster = dataproc_operator.DataprocClusterDeleteOperator(
        task_id='delete_dataproc_cluster',
        cluster_name=cluster_name,
        # Setting trigger_rule to ALL_DONE causes the cluster to be deleted
        # even if the Dataproc job fails.
        trigger_rule=trigger_rule.TriggerRule.ALL_DONE)
    #
    # # Define DAG dependencies.
    create_dataproc_cluster >> create_bq_tables >> file_etl >> dump_data >> delete_inter_datafiles >> delete_dataproc_cluster
