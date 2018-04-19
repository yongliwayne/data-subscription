import unittest
import csv
import gzip
import json
import ccws
from ccws.configs import HOME_PATH
from ccws.test.cons import TestConfigs


class Test(unittest.TestCase):
    def __init__(self, ex, currency, mode):
        unittest.TestCase.__init__(self)
        self.configs = TestConfigs[ex][currency][mode]
        self.ex = getattr(ccws, ex)()
        self.ex.set_market(currency, mode)
        self.ex.connect_redis()

    def write_into_redis(self):
        fn = self.configs['OriginFileName']
        rdk = self.ex.Config.get('RedisCollectKey')
        fd = gzip.open(fn, 'rt')
        for msg in fd:
            msg = json.loads(msg)
            self.ex.RedisConnection.lpush(rdk, json.dumps(msg))
        fd.close()

    def process_data(self):
        self.ex.process_data()

    def write_into_csv(self):
        self.ex.write_data_csv()

    def compare_two_csv(self):
        origin_date = self.configs['OriginDate']
        origin_file_path = self.configs['OriginFilePath']
        fn = self.ex.Config.get('FileName')
        with open(origin_file_path, 'rt') as fn1, \
                open('%s/%s/%s' % (HOME_PATH, origin_date, fn), 'rt') as fn2:
            reader1 = csv.DictReader(fn1)
            reader2 = csv.DictReader(fn2)
            for row1 in reader1:
                row2 = reader2.__next__()
                for k in row1.keys():
                    self.assertEqual(row1[k], row2.get(k))

