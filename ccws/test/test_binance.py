from interruptingcow import timeout
from ccws.binance import Binance
from ccws.test.test_base import Test
from ccws.configs import HOME_PATH


class TestBinance(Test, Binance):
    def __init__(self, *args, **kwargs):
        Binance.__init__(self)
        Test.__init__(self, *args, **kwargs)

    def test_BTC_USD_order(self):
        origin = {
            'FileName': 'BTC_USD-binance_order.gz',
            'Date': '2018/06/06',
            'Output': 'BTC_USD-binance.book.csv.gz',
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

    def test_BTC_USD_ticker(self):
        origin = {
            'FileName': 'BTC_USD-binance_ticker.gz',
            'Date': '2018/06/06',
            'Output': 'BTC_USD-binance.ticker.csv.gz',
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
            with timeout(5, exception=RuntimeWarning):
                self.write_data_csv()
        except RuntimeWarning:
            pass

        fn1 = origin['Output']
        fn2 = '%s/%s/%s' % (HOME_PATH, origin['Date'], self.Config['FileName'])
        self.compare_two_csv(fn1, fn2)
