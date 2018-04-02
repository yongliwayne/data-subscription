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
        url = self._WebSocketAddress + kwargs.get('url_append', '')
        on_error = kwargs.get('on_error', self.on_error)
        on_close = kwargs.get('on_error', self.on_close)
        on_message = kwargs.get('on_message', self.on_message)
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

        return sum(book, [])

    @staticmethod
    def fmt_date(ts):
        return datetime.datetime.fromtimestamp(ts / 1000, TIMEZONE).strftime('%Y-%m-%d %H:%M:%S.%f %z')

    @staticmethod
    def date_from_str(ts):
        return pytz.utc.localize(datetime.datetime.strptime(ts, '%Y-%m-%dT%H:%M:%S.%fZ'))

    # @staticmethod
    # def float_list_compare(l1, l2):
    #     if isinstance(l1, (float, int)) and isinstance(l2, (float, int)):
    #         return abs(l1 - l2) < 1e-12
    #     if isinstance(l1, str) and isinstance(l2, str):
    #         return l1 == l2
    #     if len(l1) != len(l2):
    #         return False
    #     if not l1 and not l2:
    #         return True
    #     return float_list_compare(l1[0], l2[0]) and float_list_compare(l1[1:], l2[1:])

    # def check_duplicate_data(self):
    #     rdk = self.Config['RedisQueueKey']
    #     data = json.loads(self.RedisConnection.rpop(rdk).decode('utf8'))
    #     ls = json.loads(self.RedisConnection.lrange(rdk, 0, -1).decode('utf8'))
    #     for l in ls:
    #         if configs.float_list_compare(data[1:], l[1:]):
    #             return [True, data]
    #     return [False, data]
