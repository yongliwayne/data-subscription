import websocket
import json
import time

url = 'wss://api.gemini.com/v1/marketdata/BTCUSD'
#url = 'wss://www.bitmex.com/'
#url = 'wss://www.bitmex.com/realtime'
#url = 'wss://www.bitmex.com/realtime?subscribe=instrument,orderBook:XBTUSD'
#url = 'wss://www.bitmex.com/realtime?subscribe=orderBookL2:XBTUSD,trade:XBTUSD'
#url = 'wss://www.bitmex.com/realtime?subscribe=orderBook10:XBTUSD'
#url = 'wss://ws-feed.gdax.com'
sub = {
    'type': 'subscribe',
    'channels': [{'name': 'ticker', 'product_ids': ['BTC-USD']}],
}

def on_message(ws, msg):
    ts = int(time.time() * 1000)
    print(json.dumps([ts, msg]))

def on_open(ws):
    pass
    #print('open')
    #ws.send(json.dumps(sub))

def on_close(ws):
    pass
    #print('close')

app = websocket.WebSocketApp(
    url,
    on_message=on_message,
    on_open=on_open,
    on_close=on_close,
)

app.run_forever()
