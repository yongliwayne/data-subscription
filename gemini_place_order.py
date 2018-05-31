import requests
import base64
import hmac
import hashlib
import json


API_INFO = {
    'key': 'JldRrfGJe73zZ0Z4q3tD',
    'secret': 'Bez1tswWAGTobQA4ncmAU9JmmTT',
}
base_url = 'https://api.sandbox.gemini.com'


def request_header(payload):
    encoded_payload = json.dumps(payload)
    b64 = base64.b64encode(encoded_payload)
    signature = hmac.new(API_INFO['secret'], b64, hashlib.sha384).hexdigest()
    request_headers = {
        'Content-Type': "text/plain",
        'Content-Length': "0",
        'X-GEMINI-APIKEY': API_INFO['key'],
        'X-GEMINI-PAYLOAD': base64.b64encode(encoded_payload),
        'X-GEMINI-SIGNATURE': signature,
        'Cache-Control': "no-cache"
    }
    return request_headers


def place_order(symbol, amount, price, side):
    url = base_url + '/v1/order/new'
    payload = {
        "request": "/v1/order/new",
        "nonce": 222222222222222222222,
        "client_order_id": "20150102-4738721",
        "symbol": symbol,
        "amount": amount,
        "price": price,
        "side": side,
        "type": "exchange limit",
        "options": ["maker-or-cancel"]
    }
    response = requests.post(url,
                             data=None,
                             headers=request_header(payload),
                             verify=False)
    print response.content


def cancel_order(id=''):
    if id == '':
        url = base_url + '/v1/order/cancel/all'
        payload = {
            "nonce": 1519837931379240960,
            "request": "/v1/order/cancel/all"
        }
    else:
        url = base_url + '/v1/order/cancel'
        payload = {
            "nonce": 1519837931379240960,
            "request": "/v1/order/cancel/all",
            "order_id": id
        }
    response = requests.post(url,
                             data=None,
                             headers=request_header(payload),
                             verify=False)
    print response.content


def order_status(id=''):
    if id == '':
        url = base_url + '/v1/orders'
        payload = {
            "nonce": 1519837931379240960,
            "request": "/v1/orders"
        }
    else:
        url = base_url + '/v1/order/status'
        payload = {
            "nonce": 1519837931379240960,
            "request": "/v1/order/status",
            "order_id": id
        }
    response = requests.post(url,
                             data=None,
                             headers=request_header(payload),
                             verify=False)
    print response.content

