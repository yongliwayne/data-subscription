# coding=utf-8

import json
import time
from ccws.configs import REDIS_CACHE_LENGTH
from ccws.configs import ORDER_BOOK_DEPTH
from ccws import Exchange

class Gemini(Exchange):
    ExchangeId = 'Gemini'

    def collect_data(self):
        self.connect_redis()
        self.run_websocketapp(
            url_append=self.Config['url_append']
        )

    def process_gemini_trade_data(self):
        input_key = self.Config['RedisCollectKey']
        output_key = self.Config['RedisOutputKey']
        while True:
            if self.RedisConnection.llen(input_key) <= REDIS_CACHE_LENGTH:
                time.sleep(60)
                continue
            [ct, msg] = json.loads(self.RedisConnection.rpop(input_key).decode('utf-8'))
            msg = json.loads(msg)
            ts, dt, events, ty = ct, '', msg.get('events', None), msg.get('type', None)
            if ty == 'update' and events[0].get('type', None) == 'trade':
                data = [events[0].get(k) for k in self.Config['Header']]
                ts = msg.get('timestampms')
                dt = self.fmt_date(ts)
                self.RedisConnection.lpush(output_key, json.dumps([ct, ts, dt] + data))

    def process_order_book_data(self):
        input_key = self.Config['RedisCollectKey']
        output_key = self.Config['RedisOutputKey']
        initiate = False
        asks, bids = [], []
        check_index = [0, 1, 2*ORDER_BOOK_DEPTH, 2*ORDER_BOOK_DEPTH+1]
        bid_p_old, bid_v_old, ask_p_old, ask_v_old = -1, -1, -1, -1
        v_change_threshold = 0.1
        while True:
            if self.RedisConnection.llen(input_key) < REDIS_CACHE_LENGTH:
                time.sleep(60)
                continue
            [ct, msg] = json.loads(self.RedisConnection.rpop(input_key).decode('utf-8'))
            msg = json.loads(msg)
            ts, dt, events = ct, '', msg.get('events', None)
            ty=events[0].get('reason',None)
            if events[0].get('type')=='trade' and msg.get('type')=='update':
                ty='trade' 
            if ty == 'initial':
                initiate = True
                ty='Y'
                dt = self.fmt_date(ts)
                for event in events:
                    if event.get('side') == 'bid':
                        bids.append([float(event.get('price')),float(event.get('remaining'))])
                    elif event.get('side') == 'ask':
                        asks.append([float(event.get('price')),float(event.get('remaining'))])    
                bids.sort(key=lambda x: x[0])
                asks.sort(key=lambda x: x[0])
                book = self._cut_order_book(bids, asks)
                self.RedisConnection.lpush(output_key, json.dumps([ct, ts, dt, ty] + book))
            elif initiate:
                if (ty=='place') or (ty=='cancel'):
                    self.__update_order_book(bids, asks, events[0].get('side'), events[0].get('price'), events[0].get('remaining'))
                elif ty=='trade':
                    self.__update_order_book(bids, asks, events[1].get('side'), events[1].get('price'), events[1].get('remaining'))
                    print(msg)
                else:
                    continue
                book = self._cut_order_book(bids, asks)
                # only care best bid and ask change
                [bid_p_new, bid_v_new, ask_p_new, ask_v_new] = [book[i] for i in check_index]
                if self.check_price_eq(bid_p_old, bid_p_new) \
                        and self.check_price_eq(ask_p_old, ask_p_new) \
                        and abs((bid_v_new-bid_v_old)/bid_v_old) < v_change_threshold \
                        and abs((ask_v_new-ask_v_old)/ask_v_old) < v_change_threshold:
                    continue
                [bid_p_old, bid_v_old, ask_p_old, ask_v_old] = [bid_p_new, bid_v_new, ask_p_new, ask_v_new]
                ts = msg.get('timestampms')
                dt = self.fmt_date(ts)
                ty = 'N'
                self.RedisConnection.lpush(output_key, json.dumps([ct, ts, dt, ty] + book))
            else:
                self.Logger.info(msg)        

    def __update_order_book(self, bids, asks, side, price, remaining):
        if side == 'bid':
            book = bids
        else:
            book = asks
        price=float(price)
        remaining=float(remaining)
        for i in range(len(book)):
            if self.check_price_eq(price, book[i][0]):
                if remaining < self.Config['AmountMin']:
                    del book[i]
                else:
                    book[i][1] = remaining
                return
            elif price < book[i][0]:
                book.insert(i, [price,remaining])
                return
        book.insert(len(book), [price,remaining])

    def check_price_eq(self, p1, p2):
        # divide by 2 to avoid precision
        return abs(p1-p2) < self.Config['TickSize']/2
