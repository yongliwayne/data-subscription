# coding=utf-8

import websocket
import datetime
import csv
import time
import logging
import redis
import json
import copy
import pytz
from ccws.configs import REDIS_HOST
from ccws.configs import TIMEZONE
from ccws.configs import ExConfigs
from ccws.configs import HOME_PATH
from ccws.configs import ORDER_BOOK_DEPTH


class Exchange(object):
    ExchangeId = ''

    WebSocketConnection = None
    RedisConnection = None

    def __init__(self):
        self.Logger = logging.getLogger(self.ExchangeId)
        [self.ExConfig, self._WebSocketAddress] = ExConfigs[self.ExchangeId]
        self.Config = {}

    def set_market(self, currency, mode):
        self.Config = self.ExConfig[currency][mode]
        self.Logger = logging.getLogger('%s.%s.%s' % (self.ExchangeId, currency, mode))

    def run_websocketapp(self, **kwargs):
        self.Logger.info('Begin Connection')
        url = self._WebSocketAddress + kwargs.pop('url_append', '')
        on_error = kwargs.pop('on_error', self.on_error)
        on_close = kwargs.pop('on_close', self.on_close)
        on_message = kwargs.pop('on_message', self.on_message)
        self.WebSocketConnection = websocket.WebSocketApp(
            url,
            on_error=on_error,
            on_close=on_close,
            on_message=on_message,
            **kwargs,
        )
        while True:
            try:
                self.WebSocketConnection.run_forever()
            except Exception as e:
                self.Logger.exception(e)

    def on_message(self, _ws, msg):
        ts = int(time.time()*1000)
        rdk = self.Config['RedisCollectKey']
        # self.Logger.debug(msg)
        self.RedisConnection.lpush(rdk, json.dumps([ts, msg]))

    def on_error(self, _ws, error):
        self.Logger.exception(error)

    def on_close(self, _ws):
        self.Logger.info('Connection closed.')

    def connect_redis(self):
        try:
            self.RedisConnection = redis.StrictRedis(host=REDIS_HOST)
            self.RedisConnection.ping()
        except Exception as e:
            self.Logger.exception(e)

    def write_data_csv(self):
        self.connect_redis()
        [fn, rdk] = [self.Config.get(item) for item in ['FileName', 'RedisOutputKey']]
        error_count = 100
        while True:
            try:
                if self.RedisConnection.llen(rdk) > 0:
                    data = json.loads(self.RedisConnection.rpop(rdk).decode('utf8'))
                    # data[1] is timestamp
                    dt = datetime.datetime.fromtimestamp(data[1] / 1000, TIMEZONE)
                    calendar_path = '%4d/%02d/%02d' % (dt.year, dt.month, dt.day)
                    with open('%s/%s/%s' % (HOME_PATH, calendar_path, fn), 'a+') as csvFile:
                        csvwriter = csv.writer(csvFile)
                        csvwriter.writerow(data)
                else:
                    time.sleep(60)
            except Exception as e:
                self.Logger.exception(e)
                error_count -= 1
                if error_count < 0:
                    break

    def collect_data(self):
        pass

    def process_data(self):
        self.connect_redis()
        getattr(self, self.Config.get('DataHandler', object))()

    def _check_price_eq(self, p1, p2):
        # divide by 2 to avoid precision
        return abs(p1-p2) < self.Config['TickSize']/2

    def _update_order_book(self, bids, asks, side, price, remaining):
        if side == 'bid':
            book = bids
        else:
            book = asks
        for i in range(len(book)):
            if self._check_price_eq(price, book[i][0]):
                if remaining < self.Config['AmountMin']:
                    del book[i]
                else:
                    book[i][1] = remaining
                return
            elif price < book[i][0]:
                book.insert(i, [price, remaining])
                return
        book.insert(len(book), [price, remaining])

    @staticmethod
    def _cut_order_book(bids, asks):
        if len(bids) >= ORDER_BOOK_DEPTH:
            book = bids[-ORDER_BOOK_DEPTH:]
            book.reverse()
        else:
            book = copy.deepcopy(bids)
            book.reverse()
            book += [['None', 'None']] * (ORDER_BOOK_DEPTH - len(bids))

        if len(asks) >= ORDER_BOOK_DEPTH:
            book += asks[:ORDER_BOOK_DEPTH]
        else:
            book += asks + [['None', 'None']] * (ORDER_BOOK_DEPTH - len(asks))
        book = [x[0:2] for x in book]

        return sum(book, [])

    @staticmethod
    def fmt_date(ts):
        return datetime.datetime.fromtimestamp(ts / 1000, TIMEZONE).strftime('%Y-%m-%d %H:%M:%S.%f %z')

    @staticmethod
    def date_from_str(ts):
        return pytz.utc.localize(datetime.datetime.strptime(ts, '%Y-%m-%dT%H:%M:%S.%fZ'))
