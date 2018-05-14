from interruptingcow import timeout
from ccws.gemini import Gemini
from ccws.test.test_base import Test
from ccws.configs import HOME_PATH
import gzip
import csv
import datetime
import time
from ccws.configs import TIMEZONE


class TestGemini(Test, Gemini):
    def __init__(self, *args, **kwargs):
        Gemini.__init__(self)
        Test.__init__(self, *args, **kwargs)

    def test_BTC_USD_order(self):
        origin = {
            'FileName': 'BTC_USD-gemini_data.gz',
            'Date': '2018/04/24',
            'Output': 'BTC_USD-gemini.book.csv.gz',
        }
        self.initialization('BTC/USD', 'order', origin['Date'])

        input_key = self.Config['RedisCollectKey']
        self.write_into_redis(input_key, self.RedisConnection, origin['FileName'])

        try:
            with timeout(10, exception=RuntimeWarning):
                self.process_data()
        except RuntimeWarning:
            pass

        try:
            with timeout(5, exception=RuntimeWarning):
                self.write_data_csv()
        except RuntimeWarning:
            pass

        fn1 = origin['Output']
        fn2 = '%s/%s/%s' % (HOME_PATH, origin['Date'], self.Config['FileName'])
        self.compare_two_csv(fn1, fn2)

    def test_BTC_USD_order_2(self):
        origin = {
            'FileName': 'BTC_USD-gemini_data_2.gz',
            'Date': '2018/04/25',
            'Output': 'BTC_USD-gemini-2.book.csv.gz',
        }
        self.initialization('BTC/USD', 'order', origin['Date'])

        input_key = self.Config['RedisCollectKey']
        self.write_into_redis(input_key, self.RedisConnection, origin['FileName'])

        try:
            with timeout(15, exception=RuntimeWarning):
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

    def test_ETH_USD_order(self):
        origin = {
            'FileName': 'ETH_USD-gemini_data.gz',
            'Date': '2018/05/10',
            'Output': 'ETH_USD-gemini.book.csv.gz',
        }
        self.initialization('ETH/USD', 'order', origin['Date'])

        input_key = self.Config['RedisCollectKey']
        self.write_into_redis(input_key, self.RedisConnection, origin['FileName'])

        try:
            with timeout(10, exception=RuntimeWarning):
                self.process_data()
        except RuntimeWarning:
            pass

        try:
            with timeout(5, exception=RuntimeWarning):
                self.write_data_csv()
        except RuntimeWarning:
            pass

        fn1 = origin['Output']
        fn2 = '%s/%s/%s' % (HOME_PATH, origin['Date'], self.Config['FileName'])
        self.compare_two_csv(fn1, fn2)

    def test_BTC_USD_check_trade(self):
        self.set_market('BTC/USD', 'order')
        yesterday = datetime.datetime.fromtimestamp(time.time(), TIMEZONE) + datetime.timedelta(days=-1)
        fn1 = '%s/%4d/%02d/%02d/%s' % (HOME_PATH, yesterday.year, yesterday.month, yesterday.day,
                                       'BTC_USD-gemini.book.csv.gz')
        with gzip.open(fn1, 'rt') as f:
            reader = csv.DictReader(f)
            last_row = reader.__next__()
            for row in reader:
                if row['tid'] != last_row.get('tid', 0):
                    side = row['makerSide']
                    if side == 'auction' or side is None:
                        last_row = row
                        continue
                    price_tag = '%sp0' % side
                    value_tag = '%sv0' % side
                    if row[price_tag] == last_row[price_tag]:
                        self.assertTrue(abs(float(last_row[value_tag]) - float(row[value_tag]) - float(row['amount']))
                                        < self.Config['AmountMin']/2)
                        self.assertTrue(abs(float(row[price_tag]) - float(row['price'])) < self.Config['TickSize']/2)
                    else:
                        self.assertTrue(abs(float(last_row[value_tag]) - float(row['amount']))
                                        < self.Config['AmountMin']/2)
                        self.assertTrue(abs(float(last_row[price_tag]) - float(row['price']))
                                        < self.Config['TickSize']/2)
                last_row = row
