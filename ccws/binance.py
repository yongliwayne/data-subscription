# coding=utf-8

import json
import time
from ccws import Exchange


class Binance(Exchange):
    ExchangeId = 'Binance'

    def collect_data(self):
        self.connect_redis()
        self.run_websocketapp(
            url_append=self.Config['url_append']
        )

    def process_order_data(self):
        input_key = self.Config['RedisCollectKey']
        output_key = self.Config['RedisOutputKey']
        last_id = -100
        book_pre = []
        while True:
            if self.RedisConnection.llen(input_key) < 1:
                time.sleep(60)
                continue
            [ct, msg] = json.loads(self.RedisConnection.rpop(input_key).decode('utf-8'))
            ts, msg = ct, json.loads(msg)
            uid = int(msg.get('lastUpdateId', 0))
            if last_id != -100 and uid < last_id:
                self.Logger.warning('Missing Data in front of %d' % uid)
            dt = self.fmt_date(ts)
            asks, bids = msg.get('asks'), msg.get('bids')
            asks = [x[0:2] for x in asks]
            bids = [x[0:2] for x in bids]
            bids.sort(key=lambda x: x[0], reverse=True)
            asks.sort(key=lambda x: x[0])
            book = bids + asks
            book = sum(book, [])
            last_id = uid
            if book == book_pre:
                continue
            book_pre = book
            self.RedisConnection.lpush(output_key, json.dumps([ct, ts, dt] + book))

    def process_trade_data(self):
        input_key = self.Config['RedisCollectKey']
        output_key = self.Config['RedisOutputKey']
        while True:
            if self.RedisConnection.llen(input_key) < 1:
                time.sleep(60)
                continue
            [ct, msg] = json.loads(self.RedisConnection.rpop(input_key).decode('utf-8'))
            msg = json.loads(msg)
            ts = int(msg.get('T', 0))
            dt = self.fmt_date(ts)
            con = ['q', 'p', 'E', 't', 'b', 'a']
            data = [msg.get(k) for k in con]
            self.RedisConnection.lpush(output_key, json.dumps([ct, ts, dt] + data))

