from interruptingcow import timeout
from ccws.okex import Okex
from ccws.test.test_base import Test
from ccws.configs import HOME_PATH


class TestOkex(Test, Okex):
    def __init__(self, *args, **kwargs):
        Okex.__init__(self)
        Test.__init__(self, *args, **kwargs)

    def test_BTC_USDT_order(self):
        origin = {
            'FileName': 'BTC_USDT-okex_order.gz',
            'Date': '2018/06/23',
            'Output': 'BTC_USDT-okex.book.csv.gz',
        }
        self.initialization('BTC/USDT', 'order', origin['Date'])

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

        fn1 = origin['Output']
        fn2 = '%s/%s/%s' % (HOME_PATH, origin['Date'], self.Config['FileName'])
        self.compare_two_csv(fn1, fn2)

    def test_BTC_USDT_ticker(self):
        origin = {
            'FileName': 'BTC_USDT-okex_ticker.gz',
            'Date': '2018/06/23',
            'Output': 'BTC_USDT-okex.trade.csv.gz',
        }
        self.initialization('BTC/USDT', 'trade', origin['Date'])

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

        fn1 = origin['Output']
        fn2 = '%s/%s/%s' % (HOME_PATH, origin['Date'], self.Config['FileName'])
        self.compare_two_csv(fn1, fn2)

    def test_BCH_USDT_order(self):
        origin = {
            'FileName': 'BCH_USDT-okex_order.gz',
            'Date': '2018/06/26',
            'Output': 'BCH_USDT-okex.book.csv.gz',
        }
        self.initialization('BCH/USDT', 'order', origin['Date'])

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

        fn1 = origin['Output']
        fn2 = '%s/%s/%s' % (HOME_PATH, origin['Date'], self.Config['FileName'])
        self.compare_two_csv(fn1, fn2)

    def test_BCH_USDT_ticker(self):
        origin = {
            'FileName': 'BCH_USDT-okex_ticker.gz',
            'Date': '2018/06/26',
            'Output': 'BCH_USDT-okex.trade.csv.gz',
        }
        self.initialization('BCH/USDT', 'trade', origin['Date'])

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

        fn1 = origin['Output']
        fn2 = '%s/%s/%s' % (HOME_PATH, origin['Date'], self.Config['FileName'])
        self.compare_two_csv(fn1, fn2)

    def test_ETH_USDT_order(self):
        origin = {
            'FileName': 'ETH_USDT-okex_order.gz',
            'Date': '2018/06/26',
            'Output': 'ETH_USDT-okex.book.csv.gz',
        }
        self.initialization('ETH/USDT', 'order', origin['Date'])

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

        fn1 = origin['Output']
        fn2 = '%s/%s/%s' % (HOME_PATH, origin['Date'], self.Config['FileName'])
        self.compare_two_csv(fn1, fn2)

    def test_ETH_USDT_ticker(self):
        origin = {
            'FileName': 'ETH_USDT-okex_ticker.gz',
            'Date': '2018/06/26',
            'Output': 'ETH_USDT-okex.trade.csv.gz',
        }
        self.initialization('ETH/USDT', 'trade', origin['Date'])

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

        fn1 = origin['Output']
        fn2 = '%s/%s/%s' % (HOME_PATH, origin['Date'], self.Config['FileName'])
        self.compare_two_csv(fn1, fn2)
