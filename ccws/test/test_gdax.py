from interruptingcow import timeout
from ccws.gdax import Gdax
from ccws.test.test_base import Test
from ccws.configs import HOME_PATH
import gzip
import csv


class TestGdax(Test, Gdax):
    def __init__(self, *args, **kwargs):
        Gdax.__init__(self)
        Test.__init__(self, *args, **kwargs)

    def test_BTC_USD_order(self):
        origin = {
            'FileName': 'gdax_order.gz',
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

    def test_BTC_USD_ticker(self):
        origin = {
            'FileName': 'gdax_ticker.gz',
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

    @staticmethod
    def transf(dic):
        tmp = []
        for k in dic.keys():
            tmp.append(dic[k])
        return tmp

    def test_gdax_trade(self):
        self.set_market('BTC/USD', 'order')
        fn1 = '/home/applezjm/trade_test/BTC_USD-gdax.book.csv.gz'
        fn2 = '/home/applezjm/trade_test/BTC_USD-gdax.ticker.csv.gz'
        csvFile = open('tmp.csv', 'a+')
        csvwriter = csv.writer(csvFile)
        csvwriter.writerow(['reporttimestamp', 'timestamp', 'datetime'] +
            self.Config['Header'] + ['side', 'price', 'amount', 'tradetime'])
        with gzip.open(fn1, 'rt') as f1, gzip.open(fn2, 'rt') as f2:
            reader1 = csv.DictReader(f1)
            reader2 = csv.DictReader(f2)
            last_book = reader1.__next__()
            a = 0
            record = 0
            for row2 in reader2:
                record += 1
                timestamp = int(row2['timestamp'])
                side = 'bid' if row2['side'] == 'sell' else 'ask'
                price_tag = '%sp0' % side
                value_tag = '%sv0' % side
                price = float(row2['price'])
                amount = float(row2['last_size'])
                for row1 in reader1:
                    if int(row1['timestamp']) - timestamp > 2000:
                        print(record)
                        f1.seek(0)
                        b = a - 1
                        while b:
                            b -= 1
                            last_book = reader1.__next__()
                        break
                    present_book = row1
                    if present_book[price_tag] == last_book[price_tag]:
                        if abs(float(present_book[price_tag]) - price) < self.Config['TickSize'] \
                                and abs(float(last_book[value_tag]) - float(present_book[value_tag]) - amount) \
                                < self.Config['AmountMin']:
                            last_book = present_book
                            a += 1
                            csvwriter.writerow(self.transf(present_book) + [side, price, amount, timestamp])
                            break
                    else:
                        if abs(float(last_book[price_tag]) - price) < self.Config['TickSize'] \
                                and abs(float(last_book[value_tag]) - amount) < self.Config['AmountMin']:
                            last_book = present_book
                            a += 1
                            csvwriter.writerow(self.transf(present_book) + [side, price, amount, timestamp])
                            break
                    last_book = present_book
        csvFile.close()