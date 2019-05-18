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

cluster_name = 'etl-ml-cluster-{{ ds_nodash }}'
main_bucket = models.Variable.get('main_script_bucket')
gcp_project_id = models.Variable.get('gcp_project_id')
table_name = models.Variable.get('table_id')
org_table = table_name
org_dataset = models.Variable.get('org_dataset')

snapshot_dataset = 'snapshots'
snapshot_table = '{}_{{{{ ds_nodash }}}}'.format(table_name)

fl_etl_query = """
SELECT
    member_id uuid
    ,SAFE_CAST(loan_amnt AS NUMERIC) loan_amnt
    ,SAFE_CAST(SAFE.SUBSTR(term,1,2) AS NUMERIC) term
    ,SAFE_CAST(annual_inc AS NUMERIC) annual_inc
    ,ROUND(SAFE_CAST(loan_amnt AS NUMERIC) / (SAFE_CAST(SAFE.SUBSTR(term,1,2) AS NUMERIC)/12) / (SAFE_CAST(annual_inc AS NUMERIC)+0.001),2) AS debt_inc_ratio
    ,CASE
      WHEN SAFE.SUBSTR(sub_grade,1,1) = "A" THEN 700 + SAFE_CAST(SAFE.SUBSTR(sub_grade,2,1) AS NUMERIC) *10
      WHEN SAFE.SUBSTR(sub_grade,1,1) = "B" THEN 600 + SAFE_CAST(SAFE.SUBSTR(sub_grade,2,1) AS NUMERIC) *10
      WHEN SAFE.SUBSTR(sub_grade,1,1) = "C" THEN 500 + SAFE_CAST(SAFE.SUBSTR(sub_grade,2,1) AS NUMERIC) *10
      WHEN SAFE.SUBSTR(sub_grade,1,1) = "D" THEN 400 + SAFE_CAST(SAFE.SUBSTR(sub_grade,2,1) AS NUMERIC) *10
      WHEN SAFE.SUBSTR(sub_grade,1,1) = "E" THEN 300 + SAFE_CAST(SAFE.SUBSTR(sub_grade,2,1) AS NUMERIC) *10
      WHEN SAFE.SUBSTR(sub_grade,1,1) = "F" THEN 200 + SAFE_CAST(SAFE.SUBSTR(sub_grade,2,1) AS NUMERIC) *10
      ELSE 100
      END AS cc_rating

    # Target, int_rate should not be in the dataset.
    #,SAFE_CAST(int_rate AS FLOAT64) int_rate

    ,COUNT(IF(home_ownership LIKE 'MORTGAGE', 1, null)) AS home_mortgage
    ,COUNT(IF(home_ownership LIKE 'RENT', 1, null)) AS home_rent
    ,COUNT(IF(home_ownership LIKE 'OWN', 1, null)) AS home_own

    ,COUNT(IF(purpose LIKE 'debt', 1, null)) AS purpose_debt
    ,COUNT(IF(purpose LIKE 'credit', 1, null)) AS purpose_cc
    ,COUNT(IF(purpose LIKE 'home', 1, null)) AS purpose_home
    ,COUNT(IF(purpose LIKE 'purchase', 1, null)) AS purpose_purchase
    ,COUNT(IF(purpose LIKE 'business', 1, null)) AS purpose_biz
    ,COUNT(IF(purpose LIKE 'car', 1, null)) AS purpose_car
    ,COUNT(IF(purpose LIKE 'vacation', 1, null)) AS purpose_vacation
    ,COUNT(IF(purpose LIKE 'moving', 1, null)) AS purpose_moving


FROM `mark-l-240702.unioned.loan`
GROUP BY 1, 2, 3, 4, 5, 6
    """
with models.DAG(
        'etl-ml',
        default_args=default_dag_args) as dag:
    # Create a Cloud Dataproc cluster.
    create_dataproc_cluster = dataproc_operator.DataprocClusterCreateOperator(
        task_id='create_dataproc_cluster',
        storage_bucket=models.Variable.get('dataproc_bucket'),
        cluster_name=cluster_name,
        init_actions_uris=['gs://dataproc-initialization-actions/python/pip-install.sh'],
        metadata={
            'PIP_PACKAGES': 'google-cloud-bigquery==1.11.2 google-cloud-storage==1.15.0 xgboost==0.82 hdfs==2.5.2 pandas==0.24.2 gcsfs==0.2.2'},
        image_version='1.4-debian9',
        num_workers=2,
        service_account=models.Variable.get('service_account'),
        zone=models.Variable.get('dataproc_zone'),
        master_machine_type='n1-standard-1',
        worker_machine_type='n1-standard-1')

    input_bak = dataproc_operator.DataProcPySparkOperator(
        task_id='save-input-bak',
        cluster_name=cluster_name,
        main='gs://{}/bq_query.py'.format(main_bucket),
        arguments=['--sql', 'SELECT * FROM `mark-l-240702.unioned.loan`',
                   '--target-project-id', gcp_project_id,
                   '--target-dataset', snapshot_dataset,
                   '--target-table', snapshot_table,
                   '--table-overwrite']
    )

    fl_etl_table = dataproc_operator.DataProcPySparkOperator(
        task_id='fl_etl_table',
        cluster_name=cluster_name,
        # TODO: current version only supports exporting csv to dataproc-attached gcs bucket.
        main='gs://{}/bq_query.py'.format(main_bucket),
        arguments=['--sql', fl_etl_query,
                   '--target-project-id', gcp_project_id,
                   '--target-dataset', 'feature_lib_etl',
                   '--target-table', 'fl_etl_loan_{{ ds_nodash }}',
                   '--table-overwrite', '--export-to-csv',
                   '--csv-overwrite', '--export-gcs-path',
                   'gs://{}/fl_etl.csv'.format(models.Variable.get('dataproc_bucket'))]
    )

    create_dataproc_cluster >> [input_bak, fl_etl_table]
