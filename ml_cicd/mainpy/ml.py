import os
import sys

import pandas
import xgboost as xgb
from google.cloud import bigquery, storage

table_name = sys.argv[1]

df = pandas.read_csv('gs:///ml-dataproc-bucket-mark-l-240702/fl_etl.csv')

features = ['loan_amnt', 'term', 'annual_inc', 'debt_inc_ratio',
            'cc_rating', 'home_mortgage', 'home_rent', 'home_own',
            'purpose_debt', 'purpose_cc', 'purpose_home', 'purpose_purchase',
            'purpose_biz', 'purpose_car', 'purpose_vacation', 'purpose_moving']

X = df[features]
xmatirx = xgb.DMatrix(data=X)

storage_client = storage.Client()
bucket = storage_client.get_bucket('jelly-bucket')
blob = bucket.blob(blob_name='artifact/ml.model')
blob.download_to_filename(filename='/tmp/ml.model')
booster = xgb.Booster(model_file='/tmp/ml.model')
preds = booster.predict(xmatirx)

df['pred_int_rate'] = preds

client = bigquery.Client('mark-l-240702')
# TODO: remove hard code.
dataset_ref = client.create_dataset('feature_lib_ml', exists_ok=True)
table_ref = dataset_ref.table(table_name)
job_conf = bigquery.LoadJobConfig()
job_conf.write_disposition = bigquery.job.WriteDisposition.WRITE_TRUNCATE
load_job = client.load_table_from_dataframe(df, destination=table_ref)
load_job.result()
os.remove('/tmp/ml.model')
