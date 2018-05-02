import unittest
import csv
import gzip
from ccws.gemini import Gemini


class TestGeminiTrade(unittest.TestCase, Gemini):
    def __init__(self, *args, **kwargs):
        unittest.TestCase.__init__(self, *args, **kwargs)
        Gemini.__init__(self)

    def test_gemini_trade(self):
        self.set_market('BTC/USD', 'order')
        fn1 = '/home/applezjm/trade_test/BTC_USD-gemini.book.csv.gz'
        with gzip.open(fn1, 'rt') as f:
            reader = csv.DictReader(f)
            last_row = {}
            initial = True
            a = 0
            for row in reader:
                a += 1
                if row['tid'] != last_row.get('tid', 0) and not initial:
                    side = row['makerSide']
                    if side == 'auction' or side is None:
                        last_row = row
                        continue
                    price_tag = '%sp0' % side
                    value_tag = '%sv0' % side
                    if row[price_tag] == last_row[price_tag]:
                        self.assertTrue(abs(float(last_row[value_tag]) - float(row[value_tag]) - float(row['amount'])) \
                                        < self.Config['AmountMin']/2)
                        self.assertEqual(float(row[price_tag]), float(row['price']))
                    else:
                        self.assertEqual(float(last_row[value_tag]), float(row['amount']))
                        self.assertEqual(float(last_row[price_tag]), float(row['price']))
                last_row = row
                initial = False



