from interruptingcow import timeout
from ccws.gdax import Gdax
from ccws.test.test_base import Test
from ccws.configs import HOME_PATH
from ccws.configs import load_logger_config
import logging
import gzip
import csv
import datetime
import time
from ccws.configs import TIMEZONE
import subprocess


class TestGdax(Test, Gdax):
    def __init__(self, *args, **kwargs):
        Gdax.__init__(self)
        Test.__init__(self, *args, **kwargs)

    def test_BTC_USD_order(self):
        origin = {
            'FileName': 'BTC_USD-gdax_order.gz',
            'Date': '2018/04/24',
            'Output': 'BTC_USD-gdax.book.csv.gz',
        }
        self.initialization('BTC/USD', 'order', origin['Date'])

        input_key = self.Config['RedisCollectKey']
        self.write_into_redis(input_key, self.RedisConnection, origin['FileName'])

        try:
            with timeout(5, exception=RuntimeWarning):
                self.process_data()
        except RuntimeWarning:
            pass

        try:
            with timeout(2, exception=RuntimeWarning):
                self.write_data_csv()
        except RuntimeWarning:
            pass

        fn1 = origin['Output']
        fn2 = '%s/%s/%s' % (HOME_PATH, origin['Date'], self.Config['FileName'])
        self.compare_two_csv(fn1, fn2)

    def test_BCH_USD_order(self):
        origin = {
            'FileName': 'BCH_USD-gdax_order.gz',
            'Date': '2018/05/10',
            'Output': 'BCH_USD-gdax.book.csv.gz',
        }
        self.initialization('BCH/USD', 'order', origin['Date'])

        input_key = self.Config['RedisCollectKey']
        self.write_into_redis(input_key, self.RedisConnection, origin['FileName'])

        try:
            with timeout(100, exception=RuntimeWarning):
                self.process_data()
        except RuntimeWarning:
            pass

        try:
            with timeout(50, exception=RuntimeWarning):
                self.write_data_csv()
        except RuntimeWarning:
            pass

        fn1 = origin['Output']
        fn2 = '%s/%s/%s' % (HOME_PATH, origin['Date'], self.Config['FileName'])
        self.compare_two_csv(fn1, fn2)

    def test_ETH_USD_order(self):
        origin = {
            'FileName': 'ETH_USD-gdax_order.gz',
            'Date': '2018/05/10',
            'Output': 'ETH_USD-gdax.book.csv.gz',
        }
        self.initialization('ETH/USD', 'order', origin['Date'])

        input_key = self.Config['RedisCollectKey']
        self.write_into_redis(input_key, self.RedisConnection, origin['FileName'])

        try:
            with timeout(50, exception=RuntimeWarning):
                self.process_data()
        except RuntimeWarning:
            pass

        try:
            with timeout(40, exception=RuntimeWarning):
                self.write_data_csv()
        except RuntimeWarning:
            pass

        fn1 = origin['Output']
        fn2 = '%s/%s/%s' % (HOME_PATH, origin['Date'], self.Config['FileName'])
        self.compare_two_csv(fn1, fn2)

    def test_BTC_USD_ticker(self):
        origin = {
            'FileName': 'BTC_USD-gdax_ticker.gz',
            'Date': '2018/04/24',
            'Output': 'BTC_USD-gdax.ticker.csv.gz',
        }
        self.initialization('BTC/USD', 'ticker', origin['Date'])

        input_key = self.Config['RedisCollectKey']
        self.write_into_redis(input_key, self.RedisConnection, origin['FileName'])

        try:
            with timeout(5, exception=RuntimeWarning):
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

    def test_BCH_USD_ticker(self):
        origin = {
            'FileName': 'BCH_USD-gdax_ticker.gz',
            'Date': '2018/05/10',
            'Output': 'BCH_USD-gdax.ticker.csv.gz',
        }
        self.initialization('BCH/USD', 'ticker', origin['Date'])

        input_key = self.Config['RedisCollectKey']
        self.write_into_redis(input_key, self.RedisConnection, origin['FileName'])

        try:
            with timeout(20, exception=RuntimeWarning):
                self.process_data()
        except RuntimeWarning:
            pass

        try:
            with timeout(10, exception=RuntimeWarning):
                self.write_data_csv()
        except RuntimeWarning:
            pass

        fn1 = origin['Output']
        fn2 = '%s/%s/%s' % (HOME_PATH, origin['Date'], self.Config['FileName'])
        self.compare_two_csv(fn1, fn2)

    def test_ETH_USD_ticker(self):
        origin = {
            'FileName': 'ETH_USD-gdax_ticker.gz',
            'Date': '2018/05/10',
            'Output': 'ETH_USD-gdax.ticker.csv.gz',
        }
        self.initialization('ETH/USD', 'ticker', origin['Date'])

        input_key = self.Config['RedisCollectKey']
        self.write_into_redis(input_key, self.RedisConnection, origin['FileName'])

        try:
            with timeout(20, exception=RuntimeWarning):
                self.process_data()
        except RuntimeWarning:
            pass

        try:
            with timeout(10, exception=RuntimeWarning):
                self.write_data_csv()
        except RuntimeWarning:
            pass

        fn1 = origin['Output']
        fn2 = '%s/%s/%s' % (HOME_PATH, origin['Date'], self.Config['FileName'])
        self.compare_two_csv(fn1, fn2)

    def test_BTC_USD_check_trade(self):
        self.set_market('BTC/USD', 'order')
        load_logger_config('gdax_BTC_USD_check_trade_test')
        logger = logging.getLogger('gdax_BTC_USD_check_trade_test')
        yesterday = datetime.datetime.fromtimestamp(time.time(), TIMEZONE) + datetime.timedelta(days=-1)
        fn1 = '%s/%4d/%02d/%02d/%s' % (HOME_PATH, yesterday.year, yesterday.month, yesterday.day,
                                       'BTC_USD-gdax.book.csv.gz')
        fn2 = '%s/%4d/%02d/%02d/%s' % (HOME_PATH, yesterday.year, yesterday.month, yesterday.day,
                                       'BTC_USD-gdax.ticker.csv.gz')
        output = '%s/%4d/%02d/%02d/%s' % (HOME_PATH, yesterday.year, yesterday.month, yesterday.day,
                                          'BTC_USD-gdax.consolidate.book.csv')
        with gzip.open(fn1, 'rt') as f1, gzip.open(fn2, 'rt') as f2, open(output, 'a+') as csvFile:
            reader1 = csv.DictReader(f1)
            reader2 = csv.DictReader(f2)
            csvwriter = csv.writer(csvFile)
            csvwriter.writerow(['reporttimestamp', 'timestamp', 'datetime'] +
                               self.Config['Header'] + ['side', 'price', 'amount', 'tradetime'])
            last_book = reader1.__next__()
            pointer_book = 0
            end_point = 0
            missing_time = 0
            for row2 in reader2:
                timestamp = int(row2['timestamp'])
                side = 'bid' if row2['side'] == 'sell' else 'ask'
                price_tag = '%sp0' % side
                value_tag = '%sv0' % side
                price = float(row2['price'])
                amount = float(row2['last_size'])
                for row1 in reader1:
                    pointer_book += 1
                    if int(row1['timestamp']) - timestamp > 2000:
                        missing_time += 1
                        logger.info("missing match for trade %s" % str(row2))
                        f1.seek(0)
                        tmp = end_point + 1
                        while tmp:
                            tmp -= 1
                            last_book = reader1.__next__()
                        pointer_book = end_point
                        break
                    present_book = row1
                    if (present_book[price_tag] == last_book[price_tag] and
                            self.check_equal(float(present_book[price_tag]), price, self.Config['TickSize']/2)
                            and self.check_equal(float(last_book[value_tag]) - float(present_book[value_tag]),
                                                 amount, self.Config['AmountMin']/2)) \
                            or (self.check_equal(float(last_book[price_tag]), price, self.Config['TickSize']/2)
                                and self.check_equal(float(last_book[value_tag]), amount, self.Config['AmountMin']/2)):
                        last_book = present_book
                        end_point = pointer_book
                        csvwriter.writerow(
                            [present_book.get(i) for i in reader1.fieldnames] + [side, price, amount, timestamp])
                        break
                    last_book = present_book
        subprocess.call('gzip %s' % output, shell=True)
        self.assertEqual(missing_time, 0)
