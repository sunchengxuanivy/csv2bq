# csv2bq
For data ingestion part of POC


1. Please create a Composer Environment, please place flow.py under the DAGs bucket.
2. Please place all py files in mainpy folder under bucket gs://src_raw-data_bucket/, since it's hard coded of all main ref in flow.py.
3. Please direct to Airflow Console, click [Admin] --> [Variables], and import variables.json file. please adjust value accordingly.
4. Go back to Environment Airflow webserver. the DAG will automatically run. and you may manually trigger it on console.

NOTE: If you are configuring DAG in projects other than fifty-shades-of-brown, please go under bucket gs://src_raw-data_bucket/,
and adjust permission of each and every needed .py objects to let related service account have read permission.
sharing bucket is just a tactical solution. each project should have their own bucket.

As for union-all module. please create a service account under project house-of-brownies. Grand viewer permission of dataset fifty-shades-of-brown.know_when_you_are_beaten
and dataset i-love-brownies-3000.thanos.loan_2 to this service account.
Download json credential file, and set the path as env variable, like GOOGLE_APPLICATION_CREDENTIALS=/Users/sun/Downloads/house-of-brownies-02098d4edbc7.json,
then you can run script cross-union.py