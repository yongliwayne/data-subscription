from unittest import TestCase
import unittest
import csv
import gzip
import json
import subprocess


class Test(unittest.TestCase):
    def __init__(self, Exchange):
        self.ex = Exchange
        self.ex.connect_redis()

    def write_into_redis(self, fn):
        rdk = self.ex.Config.get('RedisCollectKey')
        fd = gzip.open(fn, 'r')
        for msg in fd:
            msg = msg.decode('utf-8')
            if msg == 'open\n' or msg == 'close\n':
                continue
            self.ex.RedisConnection.lpush(rdk, json.dumps([1, msg]))
        fd.close()

    def process_data(self,):
        self.ex.process_data()

    def write_into_csv(self, fn):
        rdk = self.ex.Config.get('RedisOutputKey')
        with open(fn, 'a') as csvFile:
            csvwriter = csv.writer(csvFile)
            csvwriter.writerow(['reporttimestamp', 'timestamp', 'datetime'] + self.ex.Config['Header'])
        while True:
            if self.ex.RedisConnection.llen(rdk) > 0:
                data = json.loads(self.ex.RedisConnection.rpop(rdk).decode('utf8'))
                with open(fn, 'a+') as csvFile:
                    csvwriter = csv.writer(csvFile)
                    csvwriter.writerow(data)
            else:
                break
        subprocess.call('gzip %s*' % fn, shell=True)

    def test_write_data_csv(self, fn, origin_fn):
        with gzip.open(fn, 'rt') as fn1, \
                gzip.open(origin_fn, 'rt') as fn2:
            reader1 = csv.DictReader(fn1)
            reader2 = csv.DictReader(fn2)
            for row1 in reader1:
                row2 = reader2.__next__()
                for k in row1.keys():
                    print (k)
                    print (row1[k], row2.get(k))
                    print(type(row1[k]))
                    print(type(row2.get(k)))
                    self.assertEqual(row1[k], row2.get(k))

