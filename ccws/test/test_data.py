# coding=utf-8

import argparse
from ccws.configs import load_logger_config
from ccws.test.test_base import Test


def main():
    parser = argparse.ArgumentParser(description="Archive market data from WebSocket API.")
    parser.add_argument('-e', '--exchange', metavar='exchange', required=True, help='exchange id.')
    parser.add_argument('-s', '--symbol', metavar='symbol', required=True, help='symbol of exchange.')
    parser.add_argument('-m', '--mode', metavar='mode', required=True, help='market mode.')
    parser.add_argument('-f', '--function', metavar='function', required=True, help='handle data.')
    args = parser.parse_args()

    ex, currency_pair, mode, func = args.exchange, args.symbol, args.mode, args.function

    load_logger_config(ex)
    try:
        te = Test(ex, currency_pair, mode)
        getattr(te, func)()
    except Exception as e:
        ex.Logger.exception(e)


if __name__ == '__main__':
    main()

