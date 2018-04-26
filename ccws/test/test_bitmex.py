from interruptingcow import timeout
from ccws.bitmex import Bitmex
from ccws.test.test_base import Test
from ccws.configs import HOME_PATH


class TestBitmex(Test, Bitmex):
    def __init__(self, *args, **kwargs):
        Bitmex.__init__(self)
        Test.__init__(self, *args, **kwargs)

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
