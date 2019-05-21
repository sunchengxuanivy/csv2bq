import datetime

from airflow import models
from airflow.contrib.operators import dataproc_operator
from airflow.utils import trigger_rule

"""
    This file defines an airflow DAG to feature library ETL, model scoring (predicting), and raw_input_data backup and 
    summary.
    
    This DAG flow is located in project MARK L.
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
    4. change values of "service_account", "dataproc_bucket", "main_script_bucket", "source_data_bucket" to what you
    have just created in /ml-cicd/variables.json.
    5. change other values if needed in the json file, eg, gcp_project_id, etc.
    6. import json file to Composer: log into Composer's Airflow portal --> Admin --> Variables, browse variable.json 
    and import.
    7. Upload all scripts in /ml-cicd/mainpy directly under bucket created in STEP 3.
    
    Refresh Airflow portal and DAG is good to run. You may manually trigger and try out.
    
    data foundation result is saved in table                `mark-l-240702.data_foundation.loan`
    feature library ETL result is saved in table            `mark-l-240702.feature_lib_etl.fl_etl_loan_yyyymmdd`
    Ml model scoring(predicting) result is saved in table   `mark-l-240702.feature_lib_ml.fl_ml_loan_yyyymmdd`
    raw_input_data bak result is saved in table             `mark-l-240702.model_monitoring.loan_data_yyyymmdd`
    data summary result is saved in table                   `mark-l-240702.model_monitoring.loan_data_summary`
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

cluster_name = 'model-scoring-cluster-{{ ds_nodash }}'
main_bucket = models.Variable.get('main_script_bucket')
gcp_project_id = models.Variable.get('gcp_project_id')
table_name = models.Variable.get('table_id')
org_table = table_name
org_dataset = models.Variable.get('org_dataset')

model_monitoring_dataset = 'model_monitoring'
model_monitoring_data_table = '{}_data_{{{{ ds_nodash }}}}'.format(table_name)
model_monitoring_summary_table = '{}_data_summary'.format(table_name)

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


FROM `mark-l-240702.data_foundation.loan`
GROUP BY 1, 2, 3, 4, 5, 6
    """

summary_query = """
select
  CURRENT_DATE() AS RUNNING_DATE, 
  sum(case when (term is null or trim(term) ='') then 1 else 0 end)/count(*) as term_missing_rate,
  avg(SAFE_CAST(annual_inc as numeric)) as annual_inc_average,
  avg(SAFE_CAST(loan_amnt as numeric)) as loan_amnt_average,
  sum(case when (sub_grade is null or trim(term) ='') then 1 else 0 end)/count(*) as sub_grade_missing_rate,
  sum(case when (home_ownership is null or trim(term) ='') then 1 else 0 end)/count(*) as home_ownership_missing_rate,
  sum(case when (purpose is null or trim(term) ='') then 1 else 0 end)/count(*) as purpose_missing_rate
  from `mark-l-240702.model_monitoring.loan_data_{{ ds_nodash }}`;
"""
with models.DAG(
        'model_scoring',
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
        zone=models.Variable.get('dataproc_zone'),
        master_machine_type='n1-standard-4',
        worker_machine_type='n1-standard-1')

    backup_original_data = dataproc_operator.DataProcPySparkOperator(
        task_id='backup_original_data',
        cluster_name=cluster_name,
        main='gs://{}/bq_query.py'.format(main_bucket),
        arguments=['--sql',
                   'SELECT member_id, term, annual_inc, loan_amnt, sub_grade,home_ownership, purpose FROM `mark-l-240702.data_foundation.loan`',
                   '--target-project-id', gcp_project_id,
                   '--target-dataset', model_monitoring_dataset,
                   '--target-table', model_monitoring_data_table,
                   '--table-overwrite']
    )

    fl_etl = dataproc_operator.DataProcPySparkOperator(
        task_id='fl_etl',
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

    fl_ml = dataproc_operator.DataProcPySparkOperator(
        task_id='fl_ml',
        cluster_name=cluster_name,
        main='gs://{}/ml.py'.format(main_bucket),
        arguments=['fl_ml_loan_{{ ds_nodash }}']
    )

    data_summary = dataproc_operator.DataProcPySparkOperator(
        task_id='data_summary',
        cluster_name=cluster_name,
        main='gs://{}/bq_query.py'.format(main_bucket),
        arguments=['--sql', summary_query,
                   '--target-project-id', gcp_project_id,
                   '--target-dataset', model_monitoring_dataset,
                   '--target-table', model_monitoring_summary_table]
    )

    delete_dataproc_cluster = dataproc_operator.DataprocClusterDeleteOperator(
        task_id='delete_dataproc_cluster',
        cluster_name=cluster_name,
        # Setting trigger_rule to ALL_DONE causes the cluster to be deleted
        # even if the Dataproc job fails.
        trigger_rule=trigger_rule.TriggerRule.ALL_DONE)
    create_dataproc_cluster >> [backup_original_data, fl_etl]
    fl_etl >> fl_ml
    backup_original_data >> data_summary
    [data_summary, fl_ml] >> delete_dataproc_cluster
