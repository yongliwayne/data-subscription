# coding=utf-8

import json
import time
from ccws.configs import REDIS_CACHE_LENGTH
from ccws import Exchange


class Bitmex(Exchange):
    ExchangeId = 'Bitmex'

    def collect_data(self):
        self.connect_redis()
        #rdk = self.Config['RedisCollectKey']
        #rdk_order='bitmex-BTC_USD-order_raw'
        #rdk_trade='bitmex-BTC_USD-trade_raw'
        #fd=open('bitmex-data.dat','r')
        #for msg in fd:
        #    msg=json.loads(msg)
        #    if msg.get('table', None) == 'trade':
        #        self.RedisConnection.lpush(rdk_trade, json.dumps(msg))
        #    elif msg.get('table', None) == 'orderBookL2':
        #        self.RedisConnection.lpush(rdk_order, json.dumps(msg))
        #fd.close()
        self.run_websocketapp(
            on_open=self.on_open,
        )

    def on_open(self, ws):
        ws.send(json.dumps(self.Config['Subscription']))
        self.Logger.info('Subscription')

    def process_order_data(self):
        input_key = self.Config['RedisCollectKey']
        output_key = self.Config['RedisOutputKey']
        initialized = False
        asks, bids = [], []
        book_pre = []
        while True:
            if self.RedisConnection.llen(input_key) < REDIS_CACHE_LENGTH:
                time.sleep(60)
                continue
            [ct, msg] = json.loads(self.RedisConnection.rpop(input_key).decode('utf-8'))
            msg = json.loads(msg)
            ts, ty, events = ct, msg.get('action', None), msg.get('data', None)
            dt = self.fmt_date(ts)
            if ty == 'partial' and not initialized:
                for event in events:
                    side = event.get('side', None)
                    if side == 'Buy':
                        bids.append([float(event.get(k)) for k in ['price', 'size', 'id']])
                    elif side == 'Sell':
                        asks.append([float(event.get(k)) for k in ['price', 'size', 'id']])
                bids.sort(key=lambda x: x[0])
                asks.sort(key=lambda x: x[0])
                book = self._cut_order_book(bids, asks)
                self.RedisConnection.lpush(output_key, json.dumps([ct, ts, dt, 'Y'] + book))
                initialized = True
            elif initialized:
                for event in events:
                    feature = [event.get(k, None) for k in ['side', 'price', 'size', 'id']]
                    self._update_order_book(bids, asks, feature, ty)
                    book = self._cut_order_book(bids, asks)
                    if book == book_pre:
                        continue
                    book_pre = book
                    self.RedisConnection.lpush(output_key, json.dumps([ct, ts, dt, 'N'] + book))

    def _update_order_book(self, bids, asks, side, feature, ty):
        [side, price, size, id] = feature
        if side == 'Sell':
            book = bids
        else:
            book = asks
        if ty == 'update':
            for i in range(len(book)):
                if int(id) == int(book[i][2]):
                    if size < self.Config['AmountMin']:
                        del book[i]
                    else:
                        book[i][1] = size
                    return
        elif ty == 'delete':
            for i in range(len(book)):
                if int(id) == int(book[i][2]):
                    del book[i]
                    return
        elif ty == 'insert':
            for i in range(len(book)):
                if price < book[i][0]:
                    book.insert(i, [price, size, id])
                    return
            book.insert(len(book), [price, size, id])

    def process_trade_data(self):
        input_key = self.Config['RedisCollectKey']
        output_key = self.Config['RedisOutputKey']
        initiate = False
        while True:
            if self.RedisConnection.llen(input_key) <= REDIS_CACHE_LENGTH:
                time.sleep(60)
                continue
            [ct, msg] = json.loads(self.RedisConnection.rpop(input_key).decode('utf-8'))
            msg = json.loads(msg)
            ty, events = msg.get('action', None), msg.get('data', None)
            if ty == 'partial':
                initiate = True
            if initiate:
                for event in events:
                    data = [event.get(k) for k in self.Config['Header']]
                    ts = self.date_from_str(event.get('timestamp', '2010-01-01T00:00:01.000000Z'))
                    dt = self.fmt_date(ts.timestamp() * 1000)
                    ts = int(ts.timestamp() * 1000)
                    self.RedisConnection.lpush(output_key, json.dumps([ct, ts, dt] + data))

