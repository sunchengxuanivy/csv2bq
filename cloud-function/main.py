from google.cloud import storage
import xgboost as xgb
import pandas

storage_client = storage.Client()
bucket = storage_client.get_bucket('jelly-bucket')
blob = bucket.blob(blob_name='artifact/ml.model')
blob.download_to_filename(filename='/tmp/ml.model')


def predict_int_rate(request):
    request_json = request.get_json()
    booster = xgb.Booster(model_file='/tmp/ml.model')
    data = pandas.DataFrame(data=request_json, index=[0])
    features = ['loan_amnt', 'term', 'annual_inc', 'debt_inc_ratio',
                'cc_rating', 'home_mortgage', 'home_rent', 'home_own',
                'purpose_debt', 'purpose_cc', 'purpose_home', 'purpose_purchase',
                'purpose_biz', 'purpose_car', 'purpose_vacation', 'purpose_moving']
    X = data[features]
    xmatirx = xgb.DMatrix(data=X)
    preds = booster.predict(xmatirx)

    return str(preds[0])
