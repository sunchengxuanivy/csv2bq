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
    'project_id': models.Variable.get('gcp_project_id')
}
cluster_name = 'model-performance-cluster-{{ ds_nodash }}'
main_bucket = models.Variable.get('main_script_bucket')
gcp_project_id = models.Variable.get('gcp_project_id')
table_name = models.Variable.get('table_id')
org_table = table_name
org_dataset = models.Variable.get('org_dataset')

model_monitoring_dataset = 'model_monitoring'
model_monitoring_performance_table = 'model_performance_{}'.format(table_name)
model_monitoring_actual_table = 'loan_actual_int_rate_{{ ds_nodash }}'

performance_query = """
SELECT 
  CURRENT_DATE() AS RUNNING_DATE, 
  'mark-l-240702.feature_lib_ml.fl_ml_loan_{{ ds_nodash }}' AS PREDICT,
  'mark-l-240702.model_monitoring.loan_actual_int_rate_{{ ds_nodash }}' AS ACTURAL,
  'MSE' AS INDICATER, 
  AVG(POW(ABS(int_rate-pred_int_rate),2)) AS VALUE FROM 
  (
    SELECT 
      uuid, 
      pred_int_rate, 
      int_rate 
      FROM `mark-l-240702.feature_lib_ml.fl_ml_loan_{{ ds_nodash }}` pred 
         INNER JOIN `mark-l-240702.model_monitoring.loan_actual_int_rate_{{ ds_nodash }}` act 
         on pred.uuid = act.member_id
  )
    """
with models.DAG(
        'model_performance',
        default_args=default_dag_args) as dag:
    # Create a Cloud Dataproc cluster.
    create_dataproc_cluster = dataproc_operator.DataprocClusterCreateOperator(
        task_id='create_dataproc_cluster',
        storage_bucket=models.Variable.get('dataproc_bucket'),
        cluster_name=cluster_name,
        init_actions_uris=['gs://dataproc-initialization-actions/python/pip-install.sh'],
        metadata={
            'PIP_PACKAGES': 'google-cloud-bigquery==1.11.2 google-cloud-storage==1.15.0 xgboost==0.82 hdfs==2.5.2 pandas==0.24.2 gcsfs==0.2.2 pyarrow==0.13.0'},
        image_version='1.4-debian9',
        num_workers=2,
        service_account=models.Variable.get('service_account'),
        # zone=models.Variable.get('dataproc_zone'),
        zone='asia-east1-a',
        master_machine_type='n1-standard-4',
        worker_machine_type='n1-standard-1')

    actual_data_bak = dataproc_operator.DataProcPySparkOperator(
        task_id='actual_data_bak',
        cluster_name=cluster_name,
        main='gs://{}/bq_query.py'.format(main_bucket),
        arguments=['--sql', 'select member_id, int_rate from `mark-l-240702.data_foundation.loan`',
                   '--target-project-id', gcp_project_id,
                   '--target-dataset', model_monitoring_dataset,
                   '--target-table', model_monitoring_actual_table,
                   '--table-overwrite']
    )
    model_performance = dataproc_operator.DataProcPySparkOperator(
        task_id='model_performance',
        cluster_name=cluster_name,
        main='gs://{}/bq_query.py'.format(main_bucket),
        arguments=['--sql', performance_query,
                   '--target-project-id', gcp_project_id,
                   '--target-dataset', model_monitoring_dataset,
                   '--target-table', model_monitoring_performance_table]
    )

    delete_dataproc_cluster = dataproc_operator.DataprocClusterDeleteOperator(
        task_id='delete_dataproc_cluster',
        cluster_name=cluster_name,
        # Setting trigger_rule to ALL_DONE causes the cluster to be deleted
        # even if the Dataproc job fails.
        trigger_rule=trigger_rule.TriggerRule.ALL_DONE)

    create_dataproc_cluster >> actual_data_bak >> model_performance >> delete_dataproc_cluster
