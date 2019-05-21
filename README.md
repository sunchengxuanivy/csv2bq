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
