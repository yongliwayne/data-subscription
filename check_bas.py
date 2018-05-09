import gzip
import csv
import collections
import datetime
import time
import logging
from ccws.configs import TIMEZONE
from ccws.configs import HOME_PATH
from ccws.configs import load_logger_config


def price_cmp(p1, p2, precision):
    return abs(float(p1) - float(p2)) > precision


def main(precision, timestamp):
    load_logger_config('BTC_USD_check_bas_test')
    logger = logging.getLogger('BTC_USD_check_bas_test')
    yesterday = datetime.datetime.fromtimestamp(time.time(), TIMEZONE) + datetime.timedelta(days=-1)
    fn1 = '%s/%4d/%02d/%02d/%s' % (HOME_PATH, yesterday.year, yesterday.month, yesterday.day,
                                   'BTC_USD-gdax.book.csv.gz')
    fn2 = '%s/%4d/%02d/%02d/%s' % (HOME_PATH, yesterday.year, yesterday.month, yesterday.day,
                                   'BTC_USD-gemini.book.csv.gz')
    inf = ['reporttimestamp', 'timestamp', 'bidp0', 'bidv0', 'askp0', 'askv0']
    with gzip.open(fn1, 'rt') as f1, gzip.open(fn2, 'rt') as f2:
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
                        logger.info('%s %s' % (str(row1[i] for i in inf),
                                               str(row2[i] for i in inf)))
                    p_row1 = row1
                    row1 = reader1.__next__()
                    p_row2 = row2
                    row2 = reader2.__next__()
                    continue
                if ts1 < p_ts2:
                    p_row1 = row1
                    row1 = reader1.__next__()
                    continue
                elif ts2 < p_ts1:
                    p_row2 = row2
                    row2 = reader2.__next__()
                    continue
                elif p_ts2 < ts1 < ts2:
                    closer = p_row2 if 2 * ts1 < (p_ts2 + ts2) else row2
                    if price_cmp(bid1, closer.get('bidp0', 0, precision))\
                            or price_cmp(ask1, closer.get('askp0', 0, precision)):
                        logger.info('%s %s' % (str(row1[i] for i in inf),
                                               str(closer[i] for i in inf)))
                    p_row1 = row1
                    row1 = reader1.__next__()
                    continue
                elif p_ts1 < ts2 < ts1:
                    closer = p_row1 if 2 * ts2 < (p_ts1 + ts1) else row1
                    if price_cmp(bid2, closer.get('bidp0', 0, precision))\
                            or price_cmp(ask2, closer.get('askp0', 0, precision)):
                        logger.info('%s %s' % (str(closer[i] for i in inf),
                                               str(row2[i] for i in inf)))
                    p_row2 = row2
                    row2 = reader2.__next__()
        except StopIteration:
            exit()


if __name__ == '__main__':
    main(precision = 0.01, timestamp = 'timestamp')
