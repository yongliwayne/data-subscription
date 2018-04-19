# coding=utf-8

import json
import time
from ccws.configs import REDIS_CACHE_LENGTH
from ccws import Exchange


class Gemini(Exchange):
    ExchangeId = 'Gemini'

    def collect_data(self):
        self.connect_redis()
        self.run_websocketapp(
            url_append=self.Config['url_append']
        )

    def process_order_data(self):
        input_key = self.Config['RedisCollectKey']
        output_key = self.Config['RedisOutputKey']
        initialized = False
        asks, bids = [], []
        book_pre = []
        trade_info = ['']*len(self.Config['TradeInfoHeader'])
        ss_pre = -1
        while True:
            if self.RedisConnection.llen(input_key) < 1:
                time.sleep(60)
                continue
            [ct, msg] = json.loads(self.RedisConnection.rpop(input_key).decode('utf-8'))
            msg = json.loads(msg)
            if msg.get('type') != 'update':
                continue
            ts, ss, events = msg.get('timestampms', ct), msg['socket_sequence'], msg['events']
            if ss == 0 and not initialized:
                for event in events:
                    if event.get('reason') != 'initial' and event.get('type') != 'change':
                        self.Logger.warning("unknown event %s" % event)
                        continue
                    alias = eval('%ss' % event.get('side'))
                    alias.append([float(event.get(k)) for k in ['price', 'remaining']])
                bids.sort(key=lambda x: x[0])
                asks.sort(key=lambda x: x[0])
                book = self._cut_order_book(bids, asks)
                dt = self.fmt_date(ts)
                self.RedisConnection.lpush(output_key, json.dumps([ct, ts, dt, 'Y'] + book + trade_info))
                initialized = True
                ss_pre = ss
            elif initialized:
                if ss-ss_pre != 1:
                    self.Logger.warning('Missing Data in front of %s' % events.get('eventId'))
                ss_pre = ss
                for event in events:
                    if event['type'] == 'change':
                        self._update_order_book(bids, asks,
                                                event['side'], float(event['price']), float(event['remaining']))
                        book = self._cut_order_book(bids, asks)
                        # only care best bid and ask change
                        if book == book_pre:
                            continue
                        book_pre = book
                        dt = self.fmt_date(ts)
                        self.RedisConnection.lpush(output_key, json.dumps([ct, ts, dt, 'N'] + book + trade_info))
                    elif event['type'] == 'trade':
                        trade_info = [event.get(k) for k in self.Config['TradeInfoHeader']]
                    else:
                        self.Logger.info('un-processed event %s' % event)
