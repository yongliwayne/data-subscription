import gzip
import csv
import collections
import datetime
import time
from ccws.configs import TIMEZONE
from ccws.configs import HOME_PATH


def price_cmp(p1, p2, precision):
    return abs(float(p1) - float(p2)) / abs(float(p1) + float(p2)) > precision


def main(precision, timestamp):
    yesterday = datetime.datetime.fromtimestamp(time.time(), TIMEZONE) + datetime.timedelta(days=-1)
    fn1 = '%s/%4d/%02d/%02d/%s' % (HOME_PATH, yesterday.year, yesterday.month, yesterday.day,
                                   'BTC_USD-gdax.book.csv.gz')
    fn2 = '%s/%4d/%02d/%02d/%s' % (HOME_PATH, yesterday.year, yesterday.month, yesterday.day,
                                   'BTC_USD-gemini.book.csv.gz')
    output = '%s/%4d/%02d/%02d/%s' % (HOME_PATH, yesterday.year, yesterday.month, yesterday.day,
                                      'BTC_USD.check_bas.book.csv')
    inf = ['reporttimestamp', 'timestamp', 'bidp0', 'bidv0', 'askp0', 'askv0']
    with gzip.open(fn1, 'rt') as f1, gzip.open(fn2, 'rt') as f2, open(output, 'a+') as csvFile:
        csvwriter = csv.writer(csvFile)
        csvwriter.writerow(inf * 2)
        reader1 = csv.DictReader(f1)
        reader2 = csv.DictReader(f2)
        p_row1 = collections.OrderedDict()
        p_row2 = collections.OrderedDict()
        row1 = reader1.__next__()
        row2 = reader2.__next__()
        try:
            while True:
                p_ts1, ts1 = int(p_row1.get(timestamp, 0)), int(row1[timestamp])
                p_ts2, ts2 = int(p_row2.get(timestamp, 0)), int(row2[timestamp])
                bid1 = row1['bidp0']
                bid2 = row2['bidp0']
                ask1 = row1['askp0']
                ask2 = row2['askp0']
                if ts1 == ts2:
                    if price_cmp(bid1, bid2, precision) or price_cmp(ask1, ask2, precision):
                        csvwriter.writerow([row1[i] for i in inf] + [row2[i] for i in inf])
                    p_row1 = row1
                    row1 = reader1.__next__()
                    p_row2 = row2
                    row2 = reader2.__next__()
                elif ts1 < p_ts2:
                    p_row1 = row1
                    row1 = reader1.__next__()
                elif ts2 < p_ts1:
                    p_row2 = row2
                    row2 = reader2.__next__()
                elif p_ts2 < ts1 < ts2:
                    if price_cmp(bid1, p_row2.get('bidp0', 0), precision)\
                            or price_cmp(ask1, p_row2.get('askp0', 0), precision):
                        csvwriter.writerow([row1[i] for i in inf] + [p_row2[i] for i in inf])
                    p_row1 = row1
                    row1 = reader1.__next__()
                elif p_ts1 < ts2 < ts1:
                    if price_cmp(bid2, p_row1.get('bidp0', 0), precision)\
                            or price_cmp(ask2, p_row1.get('askp0', 0), precision):
                        csvwriter.writerow([p_row1[i] for i in inf] + [row2[i] for i in inf])
                    p_row2 = row2
                    row2 = reader2.__next__()
        except StopIteration:
            exit()


if __name__ == '__main__':
    main(precision=0.01, timestamp='timestamp')
