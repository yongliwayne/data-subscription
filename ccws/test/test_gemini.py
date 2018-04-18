from ccws.test.test_base import Test
import unittest


class TestGemini(Test, unittest.TestCase):

    def test(self, fn):
        #self.write_into_redis('gemini_data.gz')
        #self.process_data()
        #self.write_into_csv(fn)
        origin_fn = 'gemini_origin.csv.gz'
        self.test_write_data_csv(fn, origin_fn)
