from ccws.test.test_base import Test


class TestGdax(Test):
    def test_ticker(self, fn):
        self.write_into_redis('gdax_trade.tar.gz')
        self.process_data()
        self.write_into_csv(fn)
        origin_fn = 'gdax_trade_origin.csv'
        self.test_write_data_csv(fn, origin_fn)

    def test_order(self, fn):
        self.write_into_redis('gdax_book.tar.gz')
        self.process_data()
        self.write_into_csv(fn)
        origin_fn = 'gdax_book_origin.csv'
        self.test_write_data_csv(fn, origin_fn)
