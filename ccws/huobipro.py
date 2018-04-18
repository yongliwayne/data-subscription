# coding=utf-8

import json
import gzip
import time
from ccws import Exchange


class Huobipro(Exchange):
    ExchangeId = 'Huobipro'

    def collect_data(self):
        self.connect_redis()
        self.run_websocketapp(
            on_open=self.on_open,
        )

    def on_open(self, ws):
        ws.send(json.dumps(self.Config['Subscription']))
        self.Logger.info('Subscription')

    def on_message(self, ws, message):
        ts = int(time.time() * 1000)
        msg = json.loads(gzip.decompress(message).decode('utf-8'))
        if msg.get('ping', None) is None:
            if msg.get('status', 'ok') != 'ok':
                self.Logger.exception(msg)
            else:
                # self.Logger.info(msg)
                self.RedisConnection.lpush(self.Config['RedisCollectKey'], json.dumps([ts, msg]))
        else:
            ws.send(json.dumps({'pong': msg['ping']}))
            ws.send(json.dumps(self.Config['Subscription']))

    def process_order_book_data(self):
        input_key = self.Config['RedisCollectKey']
        output_key = self.Config['RedisOutputKey']
        book_pre = []
        while True:
            if self.RedisConnection.llen(input_key) < 1:
                time.sleep(60)
                continue
            [ct, msg] = json.loads(self.RedisConnection.rpop(input_key).decode('utf-8'))
            tick, ts = msg.get('tick', None), msg.get('ts', 0)
            if not tick:
                continue
            bids, asks = tick.get('bids', [[]]), tick.get('asks', [[]])
            bids.sort(key=lambda x: x[0])
            asks.sort(key=lambda x: x[0])
            book = self._cut_order_book(bids, asks, self.Config['OrderBookDepth'])
            if book_pre == book:
                continue
            book_pre = book
            dt = self.fmt_date(ts)
            self.RedisConnection.lpush(output_key, json.dumps([ct, ts, dt] + book))

    def process_trade_data(self):
        input_key = self.Config['RedisCollectKey']
        output_key = self.Config['RedisOutputKey']
        dump = []
        while True:
            if self.RedisConnection.llen(input_key) < 1:
                time.sleep(60)
                continue
            [ct, msg] = json.loads(self.RedisConnection.rpop(input_key).decode('utf-8'))
            tick = msg.get('tick', None)
            if not tick:
                continue
            data = tick.get('data', [])
            for d in data:
                dd = [d.get(k, None) for k in self.Config['Header']]
                if dump == dd:
                    continue
                ts = d.get('ts', 0)
                dt = self.fmt_date(ts)
                self.RedisConnection.lpush(output_key, json.dumps([ct, ts, dt] + dd))
                dump = dd
