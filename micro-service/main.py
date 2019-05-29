import json

import requests
from flask import Flask
from flask import request

app = Flask(__name__)


@app.route('/predict', methods=['POST'])
def predict():
    data = request.json
    headers = {
        'Content-Type': "application/json",
        'User-Agent': "PostmanRuntime/7.13.0",
        'Accept': "*/*",
        'Cache-Control': "no-cache",
        'Postman-Token': "bf6fd317-aec7-46d7-9d1e-46ab2d4f066e,d5359e00-f675-49ca-bdd5-37f5cb00d835",
        'Host': "asia-northeast1-mark-l-240702.cloudfunctions.net",
        'accept-encoding': "gzip, deflate",
        'content-length': "340",
        'Connection': "keep-alive",
        'cache-control': "no-cache"
    }
    response = requests.request("POST", "https://asia-northeast1-mark-l-240702.cloudfunctions.net/predict-int-rate",
                                data=json.dumps(data), headers=headers)
    return response.text, response.status_code


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8080, debug=True)
