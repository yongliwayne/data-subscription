class A(object):
    def t(self):
        print (1)

class B(A):
    def t(self):
        print (2)

class C:
    def __init__(self, A):
        self.x=A()
        self.x.t(self)


from ccws.test.test_gemini import TestGemini
from ccws.gemini import Gemini
from ccws.test import Test
ex = Gemini()
ex.set_market('BTC/USD', 'order')
a = Test(ex)
a.test_write_data_csv('gemini_test.csv.gz', 'gemini_origin.csv.gz')

# import unittest
# class TT(unittest.TestCase):
#     def test_a(self):
#         self.assertEqual(1,1)
#
# # unittest.main()
# t = TT()
# t.test_a()
