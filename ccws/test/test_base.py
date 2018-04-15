from unittest import TestCase
import csv
import gzip
import json
import time


class Test(TestCase):
    def write_into_redis(self, ex, fn):
        #ex = ccws.Gemini()
        #ex.set_market('BTC/USD', 'order')
        ex.connect_redis()
        rdk = ex.Config.get('RedisCollectKey')
        fd = gzip.open(fn, 'r')
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
        with open(fn, 'a') as csvFile:
            csvwriter = csv.writer(csvFile)
            csvwriter.writerow(['reporttimestamp', 'timestamp', 'datetime'] + ex.Config['Header'])
        while True:
            if ex.RedisConnection.llen(rdk) > 0:
                data = json.loads(ex.RedisConnection.rpop(rdk).decode('utf8'))
                with open(fn, 'a+') as csvFile:
                    csvwriter = csv.writer(csvFile)
                    csvwriter.writerow(data)
            else:
                time.sleep(60)

    def test_write_data_csv(self, fn, origin_fn):
        with open(fn, 'rt') as fn1, \
                open(origin_fn, 'rt') as fn2:
            reader1 = csv.DictReader(fn1)
            reader2 = csv.DictReader(fn2)
            for row1 in reader1:
                row2 = reader2.__next__()
                for k in row1.keys():
                    self.assertEqual(row1[k], row2.get(k))

