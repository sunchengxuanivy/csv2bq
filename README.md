# csv2bq
For data ingestion part of POC


/data-ingestion
    this module is for pipeline of ingesting data from GSC bucket to Bigquery table.
    DAG flow is defined in file /data-ingestion/flow.py.
    Please refer to the comments in it for detailed instructions.

/ml_cicd
    this module is for machine learning feature library ETL, and model scoring (predicting)
    it consists of 3 steps:
    data_foundation_flow.py
        cross-project union (or join if you want) tables.
    ml_flow.py
        back up raw data --> do data summary, including missing rate, and calculate column mean.
        feature library ETL --> ML model predict.
    mon_performance_flow.py
        back up actual data --> calculate MSE between predicted and acutal data.
        
    All these steps happens on dataproc. which DAG creates at the beginning and removes in the end.
    

/union-all
    @Matthew, you may ignore this part, the script is included in /ml_cicd
    Though in union_flow.py, it defines a flow to cross-project-join Bigquery tables directly from Composer, instead of
    create another dataproc to do the joining. Current Composer version available does not support Bigquery interface
    very well. eg, you are not able to create a dataset in the python script. of course, you can find another way to do it.
    I personally do not recommend this way.


As for union-all module. please create a service account under project house-of-brownies. Grand viewer permission of dataset fifty-shades-of-brown.know_when_you_are_beaten
and dataset i-love-brownies-3000.thanos.loan_2 to this service account.
Download json credential file, and set the path as env variable, like GOOGLE_APPLICATION_CREDENTIALS=/Users/sun/Downloads/house-of-brownies-02098d4edbc7.json,
then you can run script cross-union.py