import gzip
import csv


def price_cmp(p1, p2):
    return abs(float(p1) - float(p2)) < 0.01/2


def main():
    fn1 = '/home/applezjm/trade_test/BTC_USD-gdax.book.csv.gz'
    fn2 = '/home/applezjm/trade_test/BTC_USD-gemini.book.csv.gz'
    with gzip.open(fn1, 'rt') as f1, gzip.open(fn2, 'rt') as f2:
        reader1 = csv.DictReader(f1)
        reader2 = csv.DictReader(f2)
        p_row1 = reader1.__next__()
        p_row2 = reader2.__next__()
        row1 = reader1.__next__()
        row2 = reader2.__next__()
        try:
            while True:
                p_ts1, ts1 = int(p_row1['timestamp']), int(row1['timestamp'])
                p_ts2, ts2 = int(p_row2['timestamp']), int(row2['timestamp'])
                bid1 = row1['bidp0']
                bid2 = row2['bidp0']
                ask1 = row1['askp0']
                ask2 = row2['askp0']
                if ts1 < p_ts2:
                    p_row1 = row1
                    row1 = reader1.__next__()
                elif ts2 < p_ts1:
                    p_row2 = row2
                    row2 = reader2.__next__()
                elif p_ts2 <= ts1 <= ts2:
                    closer = p_row2 if 2 * ts1 < (p_ts2 + ts2) else row2
                    if not price_cmp(bid1, closer['bidp0']):
                        print('bid difference', row1, closer)
                    if not price_cmp(ask1, closer['askp0']):
                        print('ask difference', row1, closer)
                    p_row1 = row1
                    row1 = reader1.__next__()
                elif p_ts1 <= ts2 <= ts1:
                    closer = p_row1 if 2 * ts2 < (p_ts1 + ts1) else row1
                    if not price_cmp(bid2, closer['bidp0']):
                        print('bid difference', row2, closer)
                    if not price_cmp(ask2, closer['askp0']):
                        print('ask difference', row2, closer)
                    p_row2 = row2
                    row2 = reader2.__next__()
        except StopIteration:
            exit()


if __name__ == '__main__':
    main()
