from interruptingcow import timeout
from ccws.bitfinex import Bitfinex
from ccws.test.test_base import Test
from ccws.configs import HOME_PATH


class TestBitfinex(Test, Bitfinex):
    def __init__(self, *args, **kwargs):
        Bitfinex.__init__(self)
        Test.__init__(self, *args, **kwargs)

    def test_BTC_USD_order(self):
        origin = {
            'FileName': 'BTC_USD-bitfinex_order.gz',
            'Date': '2018/06/28',
            'Output': 'BTC_USD-bitfinex.book.csv.gz',
        }
        self.initialization('BTC/USD', 'order', origin['Date'])

        input_key = self.Config['RedisCollectKey']
        self.write_into_redis(input_key, self.RedisConnection, origin['FileName'])

        try:
            with timeout(60, exception=RuntimeWarning):
                self.process_data()
        except RuntimeWarning:
            pass

        try:
            with timeout(60, exception=RuntimeWarning):
                self.write_data_csv()
        except RuntimeWarning:
            pass

        # fn1 = origin['Output']
        # fn2 = '%s/%s/%s' % (HOME_PATH, origin['Date'], self.Config['FileName'])
        # self.compare_two_csv(fn1, fn2)

    def test_BTC_USD_trade(self):
        origin = {
            'FileName': 'BTC_USD-bitfinex_ticker.gz',
            'Date': '2018/06/28',
            'Output': 'BTC_USD-bitfinex.trade.csv.gz',
        }
        self.initialization('BTC/USD', 'trade', origin['Date'])

        input_key = self.Config['RedisCollectKey']
        self.write_into_redis(input_key, self.RedisConnection, origin['FileName'])

        try:
            with timeout(30, exception=RuntimeWarning):
                self.process_data()
        except RuntimeWarning:
            pass

        try:
            with timeout(30, exception=RuntimeWarning):
                self.write_data_csv()
        except RuntimeWarning:
            pass
        #
        # fn1 = origin['Output']
        # fn2 = '%s/%s/%s' % (HOME_PATH, origin['Date'], self.Config['FileName'])
        # self.compare_two_csv(fn1, fn2)