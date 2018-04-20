from interruptingcow import timeout
from ccws.test.test_base import Test
import unittest
import time


class TestGemini(unittest.TestCase):
    def test_BTC_USD_order(self):
        origin = {
            'FileName': 'gemini_data.gz',
            'Date': '2018/04/19',
            'Output': 'BTC_USD-gemini.book.csv',
        }
        te = Test('Gemini', 'BTC/USD', 'order')
        te.write_into_redis(origin['FileName'])
        time.sleep(5)
        try:
            with timeout(10, exception=RuntimeError):
                te.process_data()
        except RuntimeError:
            pass
        try:
            with timeout(10, exception=RuntimeWarning):
                te.write_into_csv()
        except RuntimeWarning:
            pass
        te.compare_two_csv(origin['Date'], origin['Output'])
