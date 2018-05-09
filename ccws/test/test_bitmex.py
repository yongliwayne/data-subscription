from interruptingcow import timeout
from ccws.bitmex import Bitmex
from ccws.test.test_base import Test
from ccws.configs import HOME_PATH
from ccws.configs import load_logger_config
import collections
import logging
import gzip
import csv
import datetime
import time
from ccws.configs import TIMEZONE


class TestBitmex(Test, Bitmex):
    def __init__(self, *args, **kwargs):
        Bitmex.__init__(self)
        Test.__init__(self, *args, **kwargs)
        load_logger_config('bitmex_test')
        self.Logger = logging.getLogger('bitmex_test')

    def test_BTC_USD_trade(self):
        origin = {
            'FileName': 'bitmex_trade.gz',
            'Date': '2018/04/24',
            'Output': 'BTC_USD-bitmex.trade.csv.gz',
        }
        self.initialization('BTC/USD', 'trade', origin['Date'])

        input_key = self.Config['RedisCollectKey']
        self.write_into_redis(input_key, self.RedisConnection, origin['FileName'])

        try:
            with timeout(2, exception=RuntimeWarning):
                self.process_data()
        except RuntimeWarning:
            pass

        try:
            with timeout(1, exception=RuntimeWarning):
                self.write_data_csv()
        except RuntimeWarning:
            pass

        fn1 = origin['Output']
        fn2 = '%s/%s/%s' % (HOME_PATH, origin['Date'], self.Config['FileName'])
        self.compare_two_csv(fn1, fn2)

    def test_BTC_USD_orderbook10(self):
        origin = {
            'FileName': 'bitmex_order.gz',
            'Date': '2018/04/24',
            'Output': 'BTC_USD-bitmex.book.csv.gz',
        }
        self.initialization('BTC/USD', 'orderbook10', origin['Date'])

        input_key = self.Config['RedisCollectKey']
        self.write_into_redis(input_key, self.RedisConnection, origin['FileName'])

        try:
            with timeout(10, exception=RuntimeWarning):
                self.process_data()
        except RuntimeWarning:
            pass

        try:
            with timeout(8, exception=RuntimeWarning):
                self.write_data_csv()
        except RuntimeWarning:
            pass

        fn1 = origin['Output']
        fn2 = '%s/%s/%s' % (HOME_PATH, origin['Date'], self.Config['FileName'])
        self.compare_two_csv(fn1, fn2)

    def disable_test_BTC_USD_check_trade(self):
        self.set_market('BTC/USD', 'orderbook10')
        load_logger_config('bitmex_BTC_USD_check_trade_test')
        logger = logging.getLogger('bitmex_BTC_USD_check_trade_test')
        yesterday = datetime.datetime.fromtimestamp(time.time(), TIMEZONE) + datetime.timedelta(days=-1)
        fn1 = '%s/%4d/%02d/%02d/%s' % (HOME_PATH, yesterday.year, yesterday.month, yesterday.day,
                                       'BTC_USD-bitmex.book.csv.gz')
        fn2 = '%s/%4d/%02d/%02d/%s' % (HOME_PATH, yesterday.year, yesterday.month, yesterday.day,
                                       'BTC_USD-bitmex.trade.csv.gz')
        with gzip.open(fn1, 'rt') as f1, gzip.open(fn2, 'rt') as f2:
            reader1 = csv.DictReader(f1)
            reader2 = csv.DictReader(f2)
            last_book = reader1.__next__()
            pointer_book = 0
            pointer_trade = 0
            end_point = 0
            missing_time = 0
            last_timestamp = 0
            trade_tmp = dict()
            trade_tmp['bid'] = collections.OrderedDict()
            trade_tmp['ask'] = collections.OrderedDict()
            initial = True
            for row2 in reader2:
                pointer_trade += 1
                timestamp = int(row2['timestamp'])
                side = 'bid' if row2['side'] == 'Sell' else 'ask'
                price = float(row2['price'])
                amount = float(row2['size'])
                if timestamp == last_timestamp or initial:
                    initial = False
                    trade_tmp[side][price] = trade_tmp[side].get(price, 0) + amount
                else:
                    for row1 in reader1:
                        pointer_book += 1
                        if int(row1['timestamp']) - timestamp > 2000:
                            missing_time += 1
                            logger.info("missing match for trade %d time %d" % (pointer_trade,
                                                                                last_timestamp))
                            f1.seek(0)
                            tmp = end_point + 1
                            while tmp:
                                tmp -= 1
                                last_book = reader1.__next__()
                            pointer_book = end_point
                            break
                        present_book = row1
                        for s in ['bid', 'ask']:
                            num = 0
                            key_num = len(trade_tmp[s]) - 1
                            if key_num == -1:
                                continue
                            flag = True
                            price_tag_0 = '%sp0' % s
                            value_tag_0 = '%sv0' % s
                            for prices in trade_tmp[s].keys():
                                if num > 9:
                                    logger.info('out of book10 in time %d' % last_timestamp)
                                    break
                                price_tag = '%sp%d' % (s, num)
                                value_tag = '%sv%d' % (s, num)
                                if num < key_num and not (self.check_equal(float(last_book[price_tag]), prices,
                                                                           self.Config['TickSize']/2) and
                                                          self.check_equal(float(last_book[value_tag]),
                                                                           trade_tmp[s][prices],
                                                                           self.Config['AmountMin']/2)):
                                    flag = False
                                    break
                                elif num == key_num and not ((present_book[price_tag_0] == last_book[price_tag]
                                                              and self.check_equal(float(present_book[price_tag]),
                                                                                   prices, self.Config['TickSize']/2)
                                                              and self.check_equal(float(last_book[value_tag]) -
                                                                                   float(present_book[value_tag_0]),
                                                                                   trade_tmp[s][prices],
                                                                                   self.Config['AmountMin']/2))
                                                             or (self.check_equal(float(last_book[price_tag]), prices,
                                                                                  self.Config['TickSize']/2)
                                                                 and self.check_equal(float(last_book[value_tag]),
                                                                                      trade_tmp[s][prices],
                                                                                      self.Config['AmountMin']/2))):
                                    flag = False
                                num += 1
                        if flag:
                            last_book = present_book
                            end_point = pointer_book
                            break
                        last_book = present_book
                    for s in ['bid', 'ask']:
                        trade_tmp[s] = collections.OrderedDict()
                    trade_tmp[side][price] = trade_tmp[side].get(price, 0) + amount
                last_timestamp = timestamp
            tmp2 = 0
            for row1 in reader1:
                tmp2 += 1
                if tmp2 > 1000:
                    logger.info("missing match for last")
                    missing_time += 1
                    break
                present_book = row1
                for s in ['bid', 'ask']:
                    num = 0
                    key_num = len(trade_tmp[s]) - 1
                    if key_num == -1:
                        continue
                    flag = True
                    price_tag_0 = '%sp0' % s
                    value_tag_0 = '%sv0' % s
                    for prices in trade_tmp[s].keys():
                        if num > 9:
                            logger.info('out of book10 in time %d' % last_timestamp)
                            break
                        price_tag = '%sp%d' % (s, num)
                        value_tag = '%sv%d' % (s, num)
                        if num < key_num and not (
                                self.check_equal(float(last_book[price_tag]), prices, self.Config['TickSize']/2)
                                and self.check_equal(float(last_book[value_tag]), trade_tmp[s][prices],
                                                     self.Config['AmountMin']/2)):
                            flag = False
                            break
                        elif num == key_num and not ((present_book[price_tag_0] == last_book[price_tag]
                                                      and self.check_equal(float(present_book[price_tag]), prices,
                                                                           self.Config['TickSize']/2)
                                                      and self.check_equal(
                                    float(last_book[value_tag]) - float(present_book[value_tag_0]),
                                    trade_tmp[s][prices], self.Config['AmountMin']/2))
                                                     or (self.check_equal(float(last_book[price_tag]), prices,
                                                                          self.Config['TickSize']/2)
                                                         and self.check_equal(float(last_book[value_tag]),
                                                                              trade_tmp[s][prices],
                                                                              self.Config['AmountMin']/2))):
                            flag = False
                        num += 1
                if flag:
                    break
                last_book = present_book
        self.assertEqual(missing_time, 0)
