import json
import time
from ccws.configs import REDIS_CACHE_LENGTH
from ccws.configs import ORDER_BOOK_DEPTH
from ccws import Exchange


class Gemini(Exchange):
    ExchangeId = 'Gemini'

    def collect_data(self):
        currency = self.Currency.replace('/', '')
        self.connect_redis()
        self.run_websocketapp(
            url_append=currency
        )

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
            events = msg['events']
            for event in events:
                if event['type'] != 'change':
                    continue
                ts, dt, ty = ct, '', event.get('reason', None)
                if ty == 'initial':
                    if event['side'] == 'bid':
                        bids.append([float(event['price']), float(event['remaining'])])
                        bids.sort(key=lambda x: x[0])
                    if event['side'] == 'ask':
                        asks.append([float(event['price']), float(event['remaining'])])
                        asks.sort(key=lambda x: x[0])
                    dt = self.fmt_date(ts)
                    ty = 'Y'
                    initiate = True
                    book = self._cut_order_book(bids, asks)
                    self.RedisConnection.lpush(output_key, json.dumps([ct, ts, dt, ty] + book))
                elif initiate:
                    change = [event['side'], event['price'], event['remaining']]
                    self.__update_order_book(bids, asks, change)
                    book = self._cut_order_book(bids, asks)
                    # only care best bid and ask change
                    [bid_p_new, bid_v_new, ask_p_new, ask_v_new] = [book[i] for i in check_index]
                    if self.check_price_eq(bid_p_old, bid_p_new) \
                            and self.check_price_eq(ask_p_old, ask_p_new) \
                            and abs((bid_v_new-bid_v_old)/bid_v_old) < v_change_threshold \
                            and abs((ask_v_new-ask_v_old)/ask_v_old) < v_change_threshold:
                        continue
                    [bid_p_old, bid_v_old, ask_p_old, ask_v_old] = [bid_p_new, bid_v_new, ask_p_new, ask_v_new]
                    dt = self.fmt_date(msg.get('timestampms'))
                    ts = msg.get('timestampms')
                    ty = 'N'
                    self.RedisConnection.lpush(output_key, json.dumps([ct, ts, dt, ty] + book))
                else:
                    self.Logger.info(msg)

    def __update_order_book(self, bids, asks, change):
        if change[0] == 'bid':
            book = bids
        else:
            book = asks
        d = [float(i) for i in change[1:]]
        for i in range(len(book)):
            if self.check_price_eq(d[0], book[i][0]):
                if d[1] < self.Config['AmountMin']:
                    del book[i]
                else:
                    book[i][1] = d[1]
                return
            elif d[0] < book[i][0]:
                book.insert(i, d)
                return
        book.insert(len(book), d)

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
            ts, dt, ty = ct, '', msg.get('socket_sequence', None)
            if ty == 0:     # socket_sequence is 0 when received first event with event is initial.
                initiate = True
                continue
            elif initiate:
                event = msg['events'][0]
                if event['type'] == 'trade':
                    data = [event.get(k) for k in self.Config['Header']]
                    dt = self.fmt_date(msg.get('timestampms'))
                    ts = msg.get('timestampms')
                    self.RedisConnection.lpush(output_key, json.dumps([ct, ts, dt] + data))

    def check_price_eq(self, p1, p2):
        # divide by 2 to avoid precision
        return abs(p1-p2) < self.Config['TickSize']/2
