from interruptingcow import timeout
from ccws.test.test_base import Test
import unittest
import os
import csv
from ccws.configs import HOME_PATH
from ccws.configs import ExConfigs


class TestGemini(unittest.TestCase):
    @staticmethod
    def create_file():
        filefolder = '%s/2018/04/19' % HOME_PATH
        filename = 'BTC_USD-gemini.book.csv'
        file = '%s/%s' % (filefolder, filename)
        if os.path.exists(file):
            os.remove(file)
        with open(file, 'a') as csvFile:
            csvwriter = csv.writer(csvFile)
            csvwriter.writerow(['reporttimestamp', 'timestamp', 'datetime'] \
                               + ExConfigs['Gemini'][0]['BTC/USD']['order']['Header'])

    def test_BTC_USD_order(self):
        origin = {
            'FileName': 'ccws/test/test_data/gemini_data.gz',
            'Date': '2018/04/19',
            'Output': 'ccws/test/test_data/BTC_USD-gemini.book.csv.gz',
        }
        self.create_file()
        te = Test('Gemini', 'BTC/USD', 'order')
        te.write_into_redis(origin['FileName'])
        try:
            with timeout(15, exception=RuntimeError):
                te.process_data()
        except RuntimeError:
            pass
        try:
            with timeout(15, exception=RuntimeWarning):
                te.write_into_csv()
        except RuntimeWarning:
            pass
        te.compare_two_csv(origin['Date'], origin['Output'])
