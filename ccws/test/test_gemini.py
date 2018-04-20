from interruptingcow import timeout
from ccws.gemini import Gemini
from ccws.test.test_base import Test
from ccws.configs import HOME_PATH


class TestGemini(Test, Gemini):
    def __init__(self, *args, **kwargs):
        Gemini.__init__(self)
        Test.__init__(self, *args, **kwargs)

    def test_BTC_USD_order(self):
        origin = {
            'FileName': 'gemini_data.gz',
            'Date': '2018/04/19',
            'Output': 'BTC_USD-gemini.book.csv.gz',
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
            with timeout(15, exception=RuntimeWarning):
                self.write_data_csv()
        except RuntimeWarning:
            pass

        fn1 = origin['Output']
        fn2 = '%s/%s/%s' % (HOME_PATH, origin['Date'], self.Config['FileName'])
        self.compare_two_csv(fn1, fn2)
