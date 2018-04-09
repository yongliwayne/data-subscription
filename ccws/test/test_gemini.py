from unittest import TestCase
import csv
import gzip


class TestGemini(TestCase):
    def test_write_data_csv(self):
        with gzip.open('gemini.book.csv.gz', 'rt') as fn1, \
                gzip.open('gemini.test.book.csv.gz', 'rt') as fn2:
            reader1 = csv.DictReader(fn1)
            reader2 = csv.DictReader(fn2)
            for row1 in reader1:
                row2 = reader2.__next__()
                for k in row1.keys():
                    self.assertEqual(row1[k], row2.get(k))

