import ccws
import argparse


class GetData:
    @staticmethod
    def collect(ex):
        ex.connect_redis()
        ex.collect_data()

    @staticmethod
    def write(ex):
        ex.connect_redis()
        # file name need to be changed
        fn = open('input.txt', 'w')
        input_key = ex.Config['RedisCollectKey']
        while ex.RedisConnection.llen(input_key) > 0:
            fn.writelines(ex.RedisConnection.rpop(input_key).decode('utf-8'))
            fn.write('\n')


def main():
    parser = argparse.ArgumentParser(description="Archive market data from WebSocket API.")
    parser.add_argument('-e', '--exchange', metavar='exchange', required=True, help='exchange id.')
    parser.add_argument('-s', '--symbol', metavar='symbol', required=True, help='symbol of exchange.')
    parser.add_argument('-m', '--mode', metavar='mode', required=True, help='market mode.')
    parser.add_argument('-f', '--function', metavar='function', required=True, help='collect/write data.')
    args = parser.parse_args()

    ex, currency_pair, mode, func = args.exchange, args.symbol, args.mode, args.function

    ex = getattr(ccws, ex)()
    ex.set_market(currency_pair, mode)
    getattr(GetData, func)(ex)


if __name__ == '__main__':
    main()
