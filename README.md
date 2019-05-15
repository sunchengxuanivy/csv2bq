# csv2bq
For data ingestion part of POC


1. Please create a Composer Environment, please place flow.py under the DAGs bucket.
2. Please place all py files in mainpy folder under some bucket, and set this bucket name to variable main_script_bucket.
3. Please direct to Airflow Console, click [Admin] --> [Variables], and import variables.json file. please adjust value accordingly.
4. Go back to Environment Airflow webserver. the DAG will automatically run. and you may manually trigger it on console.

For variables, please create all buckets, service account first, and set the created to variables.
please select 4 roles for the service account specified in the variables.json
which are BigQuery Data Editor
          BigQuery Job User
          Dataproc Worker
          Storage Object Admin

As for union-all module. please create a service account under project house-of-brownies. Grand viewer permission of dataset fifty-shades-of-brown.know_when_you_are_beaten
and dataset i-love-brownies-3000.thanos.loan_2 to this service account.
Download json credential file, and set the path as env variable, like GOOGLE_APPLICATION_CREDENTIALS=/Users/sun/Downloads/house-of-brownies-02098d4edbc7.json,
then you can run script cross-union.py