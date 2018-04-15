import ccws
from ccws.test.test_base import Test


class TestGdax(Test):
    def test_ticker(self, fn):
        ex = ccws.Gdax()
        ex.set_market('BTC/USD', 'ticker')
        self.write_into_redis(ex, 'gdax_trade')
        self.process_data(ex)
        self.write_into_csv(ex, fn)
        origin_fn = 'gdax_trade_origin.csv'
        self.test_write_data_csv(fn, origin_fn)

    def test_order(self, fn):
        ex = ccws.Gdax()
        ex.set_market('BTC/USD', 'order')
        self.write_into_redis(ex, 'gdax_book')
        self.process_data(ex)
        self.write_into_csv(ex, fn)
        origin_fn = 'gdax_book_origin.csv'
        self.test_write_data_csv(fn, origin_fn)
