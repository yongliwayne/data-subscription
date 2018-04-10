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
        rdk_order='bitmex-BTC_USD-order_raw'
        rdk_trade='bitmex-BTC_USD-trade_raw'
        fd=open('bitmex-data.dat','r')
        for msg in fd:
            msg=json.loads(msg)
            if msg.get('table', None) == 'trade':
                self.RedisConnection.lpush(rdk_trade, json.dumps(msg))
            elif msg.get('table', None) == 'orderBookL2':
                self.RedisConnection.lpush(rdk_order, json.dumps(msg))
        fd.close()

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
            msg = json.loads(self.RedisConnection.rpop(input_key).decode('utf-8'))
            ty,events=msg.get('action',None),msg.get('data',None)
            if ty=='partial' and not initialized:
                for event in events:
                    side=event.get('side', None)
                    if side=='Buy':
                        asks.append([float(event.get(k)) for k in ['price', 'size', 'id']])
                    elif side=='Sell':
                        bids.append([float(event.get(k)) for k in ['price', 'size', 'id']])
                bids.sort(key=lambda x: x[0])
                asks.sort(key=lambda x: x[0])
                book = self._cut_order_book(bids, asks)
                self.RedisConnection.lpush(output_key, json.dumps(['Y'] + book))
                initialized = True
            elif initialized:
                for event in events:
                    if ty == 'update':
                        self._update_order_book(bids, asks, event['side'], ty, None, float(event['size']), float(event['id']))
                    elif ty == 'delete':
                        self._update_order_book(bids, asks, event['side'], ty, None, 0, float(event['id']))
                    elif ty == 'insert':
                        self._update_order_book(bids, asks, event['side'], ty, float(event['price']), float(event['size']), float(event['id']))
                    book = self._cut_order_book(bids, asks)
                    if book == book_pre:
                        continue
                    book_pre = book
                    self.RedisConnection.lpush(output_key, json.dumps(['N'] + book))

    def _update_order_book(self, bids, asks, side, ty, price, size, id):
        if side == 'Sell':
            book = bids
        else:
            book = asks
        if ty == 'update' or ty == 'delete':
            for i in range(len(book)):
                if int(id) == int(book[i][2]):
                    if size < self.Config['AmountMin']:
                        del book[i]
                    else:
                        book[i][1] = size
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
            msg = json.loads(self.RedisConnection.rpop(input_key).decode('utf-8'))
            ty, events = msg.get('action', None), msg.get('data', None)
            if ty == 'partial':
                initiate = True
                for event in events:
                    data = [event.get(k) for k in self.Config['Header']]
                    ts = self.date_from_str(event.get('timestamp', '2010-01-01T00:00:01.000000Z'))
                    dt = self.fmt_date(ts.timestamp() * 1000)
                    ts = int(ts.timestamp() * 1000)
                    self.RedisConnection.lpush(output_key, json.dumps([ts, dt] + data))
                continue
            elif initiate:
                for event in events:
                    data = [event.get(k) for k in self.Config['Header']]
                    ts = self.date_from_str(event.get('timestamp', '2010-01-01T00:00:01.000000Z'))
                    dt = self.fmt_date(ts.timestamp() * 1000)
                    ts = int(ts.timestamp() * 1000)
                    self.RedisConnection.lpush(output_key, json.dumps([ts, dt] + data))
