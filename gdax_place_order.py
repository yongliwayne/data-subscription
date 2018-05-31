import json, hmac, hashlib, time, requests, base64
from requests.auth import AuthBase

API_INFO = {
    'key': 'aecd603b670581881c1fec27c4a73d42',
    'secret': 'FHaxYYmzWTsGpD4Gp5uV9MyZH5Fm+PTD+IS3yM8LiTzFJNAC4/HLH07nmzS+V9Hjjm/6mZWgCNE6hVSgz3FbSw==',
    'pass': '1xyoj8o7ghjq',
}


# Create custom authentication for Exchange
class CoinbaseExchangeAuth(AuthBase):
    def __init__(self, api_key, secret_key, passphrase):
        self.api_key = api_key
        self.secret_key = secret_key
        self.passphrase = passphrase

    def __call__(self, request):
        timestamp = str(time.time())
        message = timestamp + request.method + request.path_url + (request.body or '')
        # message = message.encode('utf-8')
        hmac_key = base64.b64decode(self.secret_key)
        signature = hmac.new(hmac_key, message, hashlib.sha256)
        signature_b64 = signature.digest().encode('base64').rstrip('\n')

        request.headers.update({
            'CB-ACCESS-SIGN': signature_b64,
            'CB-ACCESS-TIMESTAMP': timestamp,
            'CB-ACCESS-KEY': self.api_key,
            'CB-ACCESS-PASSPHRASE': self.passphrase,
            'Content-Type': 'application/json'
        })
        return request


api_url = 'https://api-public.sandbox.gdax.com/'
auth = CoinbaseExchangeAuth(API_INFO['key'], API_INFO['secret'], API_INFO['pass'])


def list_accounts(account='', history='', holds=''):
    r = requests.get(api_url + 'accounts' + account + history + holds, auth=auth)
    print r.json()


def place_order(size, price, side, product_id):
    order = {
        'size': size,
        'price': price,
        'side': side,
        'product_id': product_id,
    }
    r = requests.post(api_url + 'orders', json=order, auth=auth)
    print r.json()


def cancel_order(account=''):
    r = requests.delete(api_url + 'orders/' + account, auth=auth)
    print r.json()


def list_orders(account=''):
    r = requests.get(api_url + 'orders/' + account, auth=auth)
    print r.json()


if __name__ == '__main__':
    # place_order('0.01', '5.000', 'buy', 'BTC-USD')
    cancel_order(account='0c27a1c2-430a-4431-9174-a17266eab662')




