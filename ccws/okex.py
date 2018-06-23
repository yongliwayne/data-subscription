# coding=utf-8

import json
import time
from ccws import Exchange


class Okex(Exchange):
    ExchangeId = 'Okex'

    def collect_data(self):
        self.connect_redis()
        self.run_websocketapp(
            on_open=self.on_open,
        )

    def on_open(self, ws):
        ws.send(json.dumps(self.Config['Subscription']))
        self.Logger.info('Subscription')

    def process_order_data(self):
        input_key = self.Config['RedisCollectKey']
        output_key = self.Config['RedisOutputKey']
        initial = False
        while True:
            if self.RedisConnection.llen(input_key) < 1:
                time.sleep(60)
                continue
            [ct, msg] = json.loads(self.RedisConnection.rpop(input_key).decode('utf-8'))
            msg = json.loads(msg)[0]
            if msg.get('channel', None) == 'addChannel':
                initial = True
                continue
            elif initial:
                data = msg.get('data', None)
                ts = data.get('timestamp', 0)
                dt = self.fmt_date(ts)
                asks, bids = data.get('asks'), data.get('bids')
                bids.sort(key=lambda x: x[0], reverse=True)
                asks.sort(key=lambda x: x[0])
                book = bids + asks
                book = sum(book, [])
                self.RedisConnection.lpush(output_key, json.dumps([ct, ts, dt] + book))

    def process_trade_data(self):
        input_key = self.Config['RedisCollectKey']
        output_key = self.Config['RedisOutputKey']
        while True:
            if self.RedisConnection.llen(input_key) < 1:
                time.sleep(60)
                continue
            [ct, msg] = json.loads(self.RedisConnection.rpop(input_key).decode('utf-8'))
            msg = json.loads(msg)[0]
            if msg.get('channel', None) == 'addChannel':
                initial = True
                continue
            elif initial:
                events = msg.get('data', [])
                for event in events:
                    ts = ct
                    dt = self.fmt_date(ts)
                    self.RedisConnection.lpush(output_key, json.dumps([ct, ts, dt] + event))
