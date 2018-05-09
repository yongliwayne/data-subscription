import gzip
import csv


def main():
    fn1 = '/home/applezjm/trade_test/BTC_USD-gdax.book.csv.gz'
    fn2 = '/home/applezjm/trade_test/BTC_USD-gemini.book.csv.gz'
    with gzip.open(fn1, 'rt') as f1, gzip.open(fn2, 'rt') as f2:
        reader1 = csv.DictReader(f1)
        reader2 = csv.DictReader(f2)
        last_row2 = reader2.__next__()
        for row1 in reader1:
            ts1 = int(row1['timestamp'])
            spread1 = float(row1['askp0']) - float(row1['bidp0'])
            for row2 in reader2:
                last_ts2 = int(last_row2['timestamp'])
                ts2 = int(row2['timestamp'])
                if last_ts2 <= ts1 <= ts2:
                    if ts1 *2 == (last_ts2 + ts2):
                        spread2 = float(last_row2['askp0']) - float(last_row2['bidp0'])
                        if abs(spread1 - spread2) > 0.01 / 2:
                            print(row1, last_row2)
                        spread2 = float(row2['askp0']) - float(row2['bidp0'])
                        if abs(spread1-spread2) > 0.01 / 2:
                            print(row1, row2)
                    else:
                        closer = last_row2 if ts1 * 2 < (last_ts2 + ts2) else row2
                        spread2 = float(closer['askp0']) - float(closer['bidp0'])
                        if abs(spread1-spread2) > 0.01 / 2:
                            print(row1, closer)
                last_row2 = row2


if __name__ == '__main__':
    main()
