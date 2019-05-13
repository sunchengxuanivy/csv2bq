import datetime

from airflow import models
from airflow.contrib.operators import dataproc_operator
from airflow.utils import trigger_rule

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
    'project_id': 'fifty-shades-of-brown'
}

cluster_name = 'bucket2bq-cluster-{{ ds_nodash }}'

with models.DAG(
        'csv2bq',
        # Continue to run DAG once per day
        schedule_interval=datetime.timedelta(days=1),
        default_args=default_dag_args) as dag:
    # Create a Cloud Dataproc cluster.
    create_dataproc_cluster = dataproc_operator.DataprocClusterCreateOperator(
        task_id='create_dataproc_cluster',
        storage_bucket='dirty-dozen',
        cluster_name=cluster_name,
        init_actions_uris=['gs://dataproc-initialization-actions/jupyter/jupyter.sh',
                           'gs://dataproc-initialization-actions/python/pip-install.sh'],
        metadata={'PIP_PACKAGES': 'google-cloud-bigquery==1.11.2 google-cloud-storage==1.15.0'},
        image_version='1.4-debian9',
        num_workers=2,
        service_account='bucket-to-bigquery@fifty-shades-of-brown.iam.gserviceaccount.com',
        zone='asia-south1-b',
        master_machine_type='n1-standard-1',
        worker_machine_type='n1-standard-1')

    create_bq_tables = dataproc_operator.DataProcPySparkOperator(
        cluster_name=cluster_name,
        task_id='create_bq_tables',
        main='gs://src_raw-data_bucket/create_table.py'
    )

    file_etl = dataproc_operator.DataProcPySparkOperator(
        cluster_name=cluster_name,
        task_id='simple-file-etl',
        main='gs://src_raw-data_bucket/file_etl.py'
    )

    dump_data = dataproc_operator.DataProcPySparkOperator(
        cluster_name=cluster_name,
        task_id='csv-to-bigquery',
        main='gs://src_raw-data_bucket/csv_to_bq.py'
    )

    delete_inter_datafiles = dataproc_operator.DataProcPySparkOperator(
        cluster_name=cluster_name,
        task_id='delete-inter-datafiles',
        main='gs://src_raw-data_bucket/rm_bucket_folder.py'
    )

    # Run the Hadoop wordcount example installed on the Cloud Dataproc cluster
    # master node.
    # run_dataproc_hadoop = dataproc_operator.DataProcPySparkOperator(
    #     task_id='run_dataproc_pyspark',
    #     main='gs://src_raw-data_bucket/hello_bigquery.py',
    #     cluster_name='quickstart-cluster-{{ ds_nodash }}')

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
    # >> delete_dataproc_cluster
