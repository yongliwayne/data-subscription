import argparse
import ccws
from ccws.configs import load_logger_config


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
        ex = getattr(ccws, ex)()
        ex.set_market(currency_pair, mode)
        getattr(ex, func)()
    except Exception as e:
        ex.Logger.exception(e)


if __name__ == '__main__':
    main()
