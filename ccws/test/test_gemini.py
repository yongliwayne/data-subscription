import ccws
from ccws.test.test_base import Test


class TestGemini(Test):

    def test(self, fn):
        ex = ccws.Gemini()
        ex.set_market('BTC/USD', 'order')
        self.write_into_redis(ex, 'gemini_data')
        self.process_data(ex)
        self.write_into_csv(ex, fn)
        origin_fn = 'gemini_origin.csv'
        self.test_write_data_csv(fn, origin_fn)
