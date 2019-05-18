import datetime

from airflow import models
from airflow.contrib.operators import bigquery_operator
from airflow.operators import bash_operator

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

gcp_project_id = models.Variable.get('gcp_project_id')

with models.DAG(
        'union-loan-tables',
        default_args=default_dag_args) as dag:
    # Create a Cloud Dataproc cluster.

    # make_bq_dataset = bash_operator.BashOperator(
    #     task_id='make_bq_dataset',
    #     # Executing 'bq' command requires Google Cloud SDK which comes
    #     # preinstalled in Cloud Composer.
    #     bash_command='bq --project_id {} ls {} || bq mk {}'.format(
    #         'mark-l-240702', 'unioned', 'unioned'))

    big_query = bigquery_operator.BigQueryOperator(
        task_id='union-tables',
        use_legacy_sql=False,
        destination_dataset_table='{}.{}.{}'.format('mark-l-240702', 'unioned', 'loan'),
        sql='SELECT * FROM `festive-centaur-240702.1st_part.loan_1` UNION ALL (SELECT * FROM `mark-ii-240702.2nd_part.loan_2`);'
    )

    big_query
