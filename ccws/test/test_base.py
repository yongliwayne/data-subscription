import unittest
import csv
import gzip
import json
import os
from ccws.configs import HOME_PATH
from ccws import Exchange

datapath = './test_data' if 'ccws/test' in os.getcwd() else 'ccws/test/test_data'


class Test(unittest.TestCase, Exchange):
    def __init__(self, *args, **kwargs):
        unittest.TestCase.__init__(self, *args, **kwargs)
        Exchange.__init__(self)

    def initialization(self, currency, mode, date):
        self.set_market(currency, mode)
        self.create_test_file(date, self.Config['FileName'], self.Config['Header'])
        self.connect_redis()
        input_key = self.Config['RedisCollectKey']
        output_key = self.Config['RedisOutputKey']
        self.RedisConnection.delete(input_key)
        self.RedisConnection.delete(output_key)

    @staticmethod
    def create_test_file(path, filename, header):
        filefolder = '%s/%s' % (HOME_PATH, path)
        if not os.path.exists(filefolder):
            os.makedirs(filefolder)
        file = '%s/%s' % (filefolder, filename)
        if os.path.exists(file):
            os.remove(file)
        with open(file, 'a') as csvFile:
            csvwriter = csv.writer(csvFile)
            csvwriter.writerow(['reporttimestamp', 'timestamp', 'datetime'] + header)

    @staticmethod
    def write_into_redis(rdk, rdcon, fn):
        fn = '%s/%s' % (datapath, fn)
        fd = gzip.open(fn, 'rt')
        for msg in fd:
            msg = json.loads(msg)
            rdcon.lpush(rdk, json.dumps(msg))
        fd.close()

    def compare_two_csv(self, f1, f2):
        with gzip.open('%s/%s' % (datapath, f1), 'rt') as fn1, \
                open(f2, 'rt') as fn2:
            reader1 = csv.DictReader(fn1)
            reader2 = csv.DictReader(fn2)
            for row1, row2 in zip(reader1, reader2):
                for k in row1.keys():
                    self.assertEqual(row1[k], row2[k])

