import datetime

from airflow import models
from airflow.contrib.operators import dataproc_operator
from airflow.utils import trigger_rule

"""
    This file defines an airflow DAG to cross-project union 2 tables, one of them in project MARK I, the other MARK II,
    and save the result in project MARK L.
    
    This DAG flow is located in project MARK L.
    Please place this file in the DAG bucket after Composer is created.  
        --> thus you will see graphic view in the Airflow portal.
    
    ** The DAG will be triggered immediately after the DAG file is placed, because the startdate is set to YESTERDAY.
    
    You may have to do below items to make the Airflow DAG valid:
    1. Create a dedicated service account(dedicated for Dataproc cluster), grand role:
            BigQuery Data Editor
            BigQuery Job User
            Dataproc Worker
            Storage Object Admin  ** this role is unnecessary in this flow, but all flows are using the same service
            account to create dataproc. If you create a dedicated service account for this flow, not assigning this role
            is also good.
            
    2. Go to both dataset where both union tables lie in MARK I, MARK II, grand Bigquery Data Viewer permission to the 
    service account created.
            
    3. create GSC bucket for dataproc
    4. create another GSC bucket for placing python scripts.
    5. change values of "service_account", "dataproc_bucket", "main_script_bucket", "source_data_bucket" to what you
    have just created in /ml-cicd/variables.json.
    6. change other values if needed in the json file, eg, gcp_project_id, etc.
    7. import json file to Composer: log into Composer's Airflow portal --> Admin --> Variables, browse variable.json 
    and import.
    8. Upload all scripts in /ml-cicd/mainpy directly under bucket created in STEP 3.
    
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
cluster_name = 'collect-data-cluster-{{ ds_nodash }}'
main_bucket = models.Variable.get('main_script_bucket')
gcp_project_id = models.Variable.get('gcp_project_id')
table_name = models.Variable.get('table_id')
org_table = table_name
org_dataset = models.Variable.get('org_dataset')

data_foundation_dataset = 'data_foundation'
data_foundation_table = 'loan'

with models.DAG(
        'data_foundation',
        default_args=default_dag_args) as dag:
    # Create a Cloud Dataproc cluster.
    create_dataproc_cluster = dataproc_operator.DataprocClusterCreateOperator(
        task_id='create_dataproc_cluster',
        storage_bucket=models.Variable.get('dataproc_bucket'),
        cluster_name=cluster_name,
        init_actions_uris=['gs://dataproc-initialization-actions/python/pip-install.sh'],
        metadata={
            'PIP_PACKAGES': 'google-cloud-bigquery==1.11.2 google-cloud-storage==1.15.0'},
        image_version='1.4-debian9',
        num_workers=0,
        service_account=models.Variable.get('service_account'),
        # zone=models.Variable.get('dataproc_zone'),
        zone='asia-east1-a',
        master_machine_type='n1-standard-1')

    cross_project_fetch_data = dataproc_operator.DataProcPySparkOperator(
        task_id='data_foundation_cross_proj',
        cluster_name=cluster_name,
        main='gs://{}/bq_query.py'.format(main_bucket),
        arguments=['--sql',
                   'SELECT * FROM `festive-centaur-240702.1st_part.loan_1` UNION ALL (SELECT * FROM `mark-ii-240702.2nd_part.loan_2`);',
                   '--target-project-id', gcp_project_id,
                   '--target-dataset', data_foundation_dataset,
                   '--target-table', data_foundation_table,
                   '--table-overwrite']
    )

    delete_dataproc_cluster = dataproc_operator.DataprocClusterDeleteOperator(
        task_id='delete_dataproc_cluster',
        cluster_name=cluster_name,
        # Setting trigger_rule to ALL_DONE causes the cluster to be deleted
        # even if the Dataproc job fails.
        trigger_rule=trigger_rule.TriggerRule.ALL_DONE)

    create_dataproc_cluster >> cross_project_fetch_data >> delete_dataproc_cluster
