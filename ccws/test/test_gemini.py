from unittest import TestCase
import csv
import gzip
import json
import time
import ccws

class TestGemini(TestCase):

    def test(self, typ, fn):
        if typ == 'gemini':
            ex = ccws.Gemini()
            ex.set_market('BTC/USD', 'order')
            self.write_into_redis(ex, 'gemini_data')
        elif typ == 'gdax_book':
            ex = ccws.Gdax()
            ex.set_market('BTC/USD', 'order')
            self.write_into_redis(ex, 'gdax_book')
        elif typ == 'gdax_trade':
            ex = ccws.Gdax()
            ex.set_market('BTC/USD', 'ticker')
            self.write_into_redis(ex, 'gdax_trade')
        self.process_data(ex)
        self.write_into_csv(ex, fn)


    def write_into_redis(self, ex, fn):
        #ex = ccws.Gemini()
        #ex.set_market('BTC/USD', 'order')
        ex.connect_redis()
        rdk = ex.Config.get('RedisCollectKey')
        fd = open(fn, 'r')
        for msg in fd:
            if msg == 'open\n' or msg == 'close\n':
                continue
            ex.RedisConnection.lpush(rdk, json.dumps([1, msg]))
        fd.close()

    def process_data(self, ex):
        #ex = ccws.Gemini()
        #ex.set_market('BTC/USD', 'order')
        ex.process_data()

    def write_into_csv(self, ex, fn):
        #ex = ccws.Gemini()
        #ex.set_market('BTC/USD', 'order')
        ex.connect_redis()
        rdk = ex.Config.get('RedisOutputKey')
        while True:
            if ex.RedisConnection.llen(rdk) > 0:
                data = json.loads(ex.RedisConnection.rpop(rdk).decode('utf8'))
                with open(fn, 'a+') as csvFile:
                    csvwriter = csv.writer(csvFile)
                    csvwriter.writerow(data)
            else:
                time.sleep(60)

    def diff(self, typ, fn):
        if typ == 'gemini':
            origin_fn = 'gemini_origin.csv'
        elif typ == 'gdax_book':
            origin_fn = 'gdax_book_origin.csv'
        elif typ == 'gdax_trade':
            origin_fn = 'gdax_trade_origin.csv'
        self.test_write_data_csv(fn, origin_fn)

    def test_write_data_csv(self, fn, origin_fn):
        with open(fn, 'rt') as fn1, \
                open(origin_fn, 'rt') as fn2:
            reader1 = csv.DictReader(fn1)
            reader2 = csv.DictReader(fn2)
            for row1 in reader1:
                row2 = reader2.__next__()
                for k in row1.keys():
                    self.assertEqual(row1[k], row2.get(k))

