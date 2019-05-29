import requests

url = "https://asia-northeast1-mark-l-240702.cloudfunctions.net/predict-int-rate"

payload = """
{
  "loan_amnt": 17600,
  "term": 60,
  "annual_inc": 20000.0,
  "debt_inc_ratio": 0.18,
  "cc_rating": 220,
  "home_mortgage": 0,
  "home_rent": 1,
  "home_own": 0,
  "purpose_debt": 0,
  "purpose_cc": 0,
  "purpose_home": 0,
  "purpose_purchase": 0,
  "purpose_biz": 0,
  "purpose_car": 0,
  "purpose_vacation": 0,
  "purpose_moving": 0
}
"""
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

response = requests.request("POST", url, data=payload, headers=headers)

print(response.text)