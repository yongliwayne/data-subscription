# coding=utf-8

import json
import time
from ccws.configs import REDIS_CACHE_LENGTH
from ccws.configs import ORDER_BOOK_DEPTH
from ccws import Exchange


class Gdax(Exchange):
    ExchangeId = 'Gdax'

    def collect_data(self):
        self.connect_redis()
        self.run_websocketapp(
            on_open=self.on_open,
        )

    def on_open(self, ws):
        ws.send(json.dumps(self.Config['Subscription']))
        self.Logger.info('Subscription')

    def process_order_book_data(self):
        input_key = self.Config['RedisCollectKey']
        output_key = self.Config['RedisOutputKey']
        initiate = False
        asks, bids = [], []
        check_index = [0, 1, 2 * ORDER_BOOK_DEPTH, 2 * ORDER_BOOK_DEPTH + 1]
        bid_p_old, bid_v_old, ask_p_old, ask_v_old = -1, -1, -1, -1
        v_change_threshold = 0.1
        while True:
            if self.RedisConnection.llen(input_key) < REDIS_CACHE_LENGTH:
                #time.sleep(60)
                #continue
                break
            [ct, msg] = json.loads(self.RedisConnection.rpop(input_key).decode('utf-8'))
            msg = json.loads(msg)
            ts, dt, ty = ct, '', msg.get('type', None)
            if ty == 'snapshot':
                bids = [[float(i) for i in j] for j in msg.get('bids')]
                bids.sort(key=lambda x: x[0])
                asks = [[float(i) for i in j] for j in msg.get('asks')]
                asks.sort(key=lambda x: x[0])
                dt = self.fmt_date(ts)
                ty = 'Y'
                initiate = True
                book = self._cut_order_book(bids, asks)
                self.RedisConnection.lpush(output_key, json.dumps([ct, ts, dt, ty] + book))
            elif initiate and ty == 'l2update':
                changes = msg.get('changes', [])
                for change in changes:
                    self._update_order_book(bids, asks, change[0], float(change[1]), float(change[2]))
                book = self._cut_order_book(bids, asks)
                # only care best bid and ask change
                [bid_p_new, bid_v_new, ask_p_new, ask_v_new] = [book[i] for i in check_index]
                if self._check_price_eq(bid_p_old, bid_p_new) \
                        and self._check_price_eq(ask_p_old, ask_p_new) \
                        and abs((bid_v_new - bid_v_old) / bid_v_old) < v_change_threshold \
                        and abs((ask_v_new - ask_v_old) / ask_v_old) < v_change_threshold:
                    continue
                [bid_p_old, bid_v_old, ask_p_old, ask_v_old] = [bid_p_new, bid_v_new, ask_p_new, ask_v_new]
                ts = self.date_from_str(msg.get('time', '2010-01-01T00:00:01.000000Z'))
                dt = self.fmt_date(ts.timestamp() * 1000)
                ts = int(ts.timestamp() * 1000)
                ty = 'N'
                self.RedisConnection.lpush(output_key, json.dumps([ct, ts, dt, ty] + book))
            else:
                self.Logger.info(msg)

    def process_ticker_data(self):
        input_key = self.Config['RedisCollectKey']
        output_key = self.Config['RedisOutputKey']
        initiate = False
        while True:
            if self.RedisConnection.llen(input_key) <= REDIS_CACHE_LENGTH:
                time.sleep(60)
                continue
            [ct, msg] = json.loads(self.RedisConnection.rpop(input_key).decode('utf-8'))
            msg = json.loads(msg)
            ts, dt, ty = ct, '', msg.get('type', None)
            if ty == 'subscriptions':
                initiate = True
                continue
            elif initiate:
                data = [msg.get(k) for k in self.Config['Header']]
                ts = self.date_from_str(msg.get('time', '2010-01-01T00:00:01.000000Z'))
                dt = self.fmt_date(ts.timestamp() * 1000)
                ts = int(ts.timestamp() * 1000)
                self.RedisConnection.lpush(output_key, json.dumps([ct, ts, dt] + data))
