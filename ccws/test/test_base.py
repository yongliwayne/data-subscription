import unittest
import csv
import gzip
import json
import ccws
from ccws.configs import HOME_PATH
from createcsvfile import create_files


class Test(unittest.TestCase):
    def __init__(self, ex, currency, mode):
        unittest.TestCase.__init__(self)
        self.ex = getattr(ccws, ex)()
        self.ex.set_market(currency, mode)

    @staticmethod
    def create_test_files():
        create_files()

    def write_into_redis(self, fn):
        self.ex.connect_redis()
        rdk = self.ex.Config.get('RedisCollectKey')
        self.ex.RedisConnection.delete(rdk)
        fd = gzip.open(fn, 'rt')
        for msg in fd:
            msg = json.loads(msg)
            self.ex.RedisConnection.lpush(rdk, json.dumps(msg))
        fd.close()

    def process_data(self):
        rdk = self.ex.Config.get('RedisOutputKey')
        self.ex.RedisConnection.delete(rdk)
        self.ex.process_data()

    def write_into_csv(self):
        self.ex.write_data_csv()

    def compare_two_csv(self, date, output):
        fn = self.ex.Config.get('FileName')
        with gzip.open(output, 'rt') as fn1, \
                open('%s/%s/%s' % (HOME_PATH, date, fn), 'rt') as fn2:
            reader1 = csv.DictReader(fn1)
            reader2 = csv.DictReader(fn2)
            for row1, row2 in zip(reader1, reader2):
                for k in row1.keys():
                    self.assertEqual(row1[k], row2[k])

