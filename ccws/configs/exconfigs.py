# coding=utf-8


def order_book_header_with_depth(depth):
    return sum([['bidp%d' % i, 'bidv%d' % i] for i in range(depth)], []) \
           + sum([['askp%d' % i, 'askv%d' % i] for i in range(depth)], [])


HuobiConfigs = {
    'BTC/USDT': {
        # 'order-full': {
        #     'Subscription': {
        #         'sub': 'market.btcusdt.depth.step0',
        #         'id': 'hubiproorderbook',
        #     },
        #     'Header': [
        #         'bidsVector',
        #         'asksVector',
        #     ],
        #     'FileName': 'BTC_USDT-huobipro.order.full.csv',
        #     'RedisQueueKey': 'huobipro-BTC_USDT-order-full',
        #     'Handler': 'full_order_book_handler'
        # },

        'trade': {
            'Subscription': {
                'sub': 'market.btcusdt.trade.detail',
                'id': 'hubiprotrade',
            },
            'Header': [
                'price',
                'amount',
                'direction',
                'id',
            ],
            'FileName': 'BTC_USDT-huobipro.trade.csv',
            'RedisCollectKey': 'huobipro-BTC_USDT-trade_raw',
            'RedisOutputKey': 'huobipro-BTC_USDT-trade_processed',
            'DataHandler': 'process_trade_data',
        },

        'order': {
            'Subscription': {
                'sub': 'market.btcusdt.depth.step0',
                'id': 'hubiproorderbook',
            },
            'OrderBookDepth': 12,
            'Header': order_book_header_with_depth(12),
            'FileName': 'BTC_USDT-huobipro.book.csv',
            'RedisCollectKey': 'huobipro-BTC_USDT-order_raw',
            'RedisOutputKey': 'huobipro-BTC_USDT-order_processed',
            'DataHandler': 'process_order_book_data',
        },
    },
}

GdaxConfigs = {
    'BTC/USD': {
        'ticker': {
            'Subscription': {
                'type': 'subscribe',
                'channels': [{'name': 'ticker', 'product_ids': ['BTC-USD']}],
            },
            'Header': [
                'price',
                'last_size',
                'side',
                'best_ask',
                'best_bid',
                'high_24h',
                'low_24h',
                'open_24h',
                'volume_24h',
                'volume_30d',
                'sequence',
                'trade_id',
            ],
            'FileName': 'BTC_USD-gdax.ticker.csv',
            'RedisCollectKey': 'gdax-BTC_USD-ticker_raw',
            'RedisOutputKey': 'gdax-BTC_USD-ticker',
            'DataHandler': 'process_ticker_data',
        },

        'order': {
            'Subscription': {
                'type': 'subscribe',
                'channels': [{'name': 'level2', 'product_ids': ['BTC-USD']}],
            },
            'OrderBookDepth': 12,
            'Header': ['IsSnapShot'] + order_book_header_with_depth(12),
            'FileName': 'BTC_USD-gdax.book.csv',
            'RedisCollectKey': 'gdax-BTC_USD-order_raw',
            'RedisOutputKey': 'gdax-BTC_USD-order_processed',
            'DataHandler': 'process_order_book_data',
            'TickSize': 0.01,
            'AmountMin': 1e-8,
        },

    },

    'BCH/USD': {

        'ticker': {
            'Subscription': {
                'type': 'subscribe',
                'channels': [{'name': 'ticker', 'product_ids': ['BCH-USD']}],
            },
            'Header': [
                'price',
                'last_size',
                'side',
                'best_ask',
                'best_bid',
                'high_24h',
                'low_24h',
                'open_24h',
                'volume_24h',
                'volume_30d',
                'sequence',
                'trade_id',
            ],
            'FileName': 'BCH_USD-gdax.ticker.csv',
            'RedisCollectKey': 'gdax-BCH_USD-ticker_raw',
            'RedisOutputKey': 'gdax-BCH_USD-ticker',
            'DataHandler': 'process_ticker_data',
        },

        'order': {
            'Subscription': {
                'type': 'subscribe',
                'channels': [{'name': 'level2', 'product_ids': ['BCH-USD']}],
            },
            'OrderBookDepth': 5,
            'Header': ['IsSnapShot'] + order_book_header_with_depth(5),
            'FileName': 'BCH_USD-gdax.book.csv',
            'RedisCollectKey': 'gdax-BCH_USD-order_raw',
            'RedisOutputKey': 'gdax-BCH_USD-order_processed',
            'DataHandler': 'process_order_book_data',
            'TickSize': 0.01,
            'AmountMin': 1e-8,
        },

    },

    'ETH/USD': {

        'ticker': {
            'Subscription': {
                'type': 'subscribe',
                'channels': [{'name': 'ticker', 'product_ids': ['ETH-USD']}],
            },
            'Header': [
                'price',
                'last_size',
                'side',
                'best_ask',
                'best_bid',
                'high_24h',
                'low_24h',
                'open_24h',
                'volume_24h',
                'volume_30d',
                'sequence',
                'trade_id',
            ],
            'FileName': 'ETH_USD-gdax.ticker.csv',
            'RedisCollectKey': 'gdax-ETH_USD-ticker_raw',
            'RedisOutputKey': 'gdax-ETH_USD-ticker',
            'DataHandler': 'process_ticker_data',
        },

        'order': {
            'Subscription': {
                'type': 'subscribe',
                'channels': [{'name': 'level2', 'product_ids': ['ETH-USD']}],
            },
            'OrderBookDepth': 12,
            'Header': ['IsSnapShot'] + order_book_header_with_depth(12),
            'FileName': 'ETH_USD-gdax.book.csv',
            'RedisCollectKey': 'gdax-ETH_USD-order_raw',
            'RedisOutputKey': 'gdax-ETH_USD-order_processed',
            'DataHandler': 'process_order_book_data',
            'TickSize': 0.01,
            'AmountMin': 1e-8,
        },

    },

}

_gemini_trade_info_header = [
    'tid',
    'makerSide',
    'price',
    'amount',
]

# noinspection PyPep8
GeminiConfigs = {
    'BTC/USD': {
        'order': {
            'url_append': '/marketdata/BTCUSD',
            'OrderBookDepth': 12,
            'Header': ['IsSnapShot'] + order_book_header_with_depth(12) + _gemini_trade_info_header + ['lasttradetime'],
            'TradeInfoHeader': _gemini_trade_info_header,
            'FileName': 'BTC_USD-gemini.book.csv',
            'RedisCollectKey': 'gemini-BTC_USD_raw',
            'RedisOutputKey': 'gemini-BTC_USD_processed',
            'DataHandler': 'process_order_data',
            'TickSize': 0.01,
            'AmountMin': 1e-8,
        },
    },

    'ETH/USD': {
        'order': {
            'url_append': '/marketdata/ETHUSD',
            'OrderBookDepth': 12,
            'Header': ['IsSnapShot'] + order_book_header_with_depth(12) + _gemini_trade_info_header + ['lasttradetime'],
            'TradeInfoHeader': _gemini_trade_info_header,
            'FileName': 'ETH_USD-gemini.book.csv',
            'RedisCollectKey': 'gemini-ETH_USD_raw',
            'RedisOutputKey': 'gemini-ETH_USD_processed',
            'DataHandler': 'process_order_data',
            'TickSize': 0.01,
            'AmountMin': 1e-8,
        },
    },
}

BitmexConfigs = {
    'BTC/USD': {
        'trade': {
            'Subscription': {
                'op': 'subscribe',
                'args': ["trade:XBTUSD"],
            },
            'Header': [
                'side',
                'size',
                'price',
                'tickDirection',
                'trdMatchID',
                'grossValue',
                'homeNotional',
                'foreignNotional',
            ],
            'FileName': 'BTC_USD-bitmex.trade.csv',
            'RedisCollectKey': 'bitmex-BTC_USD-trade_raw',
            'RedisOutputKey': 'bitmex-BTC_USD-trade_processed',
            'DataHandler': 'process_trade_data',
        },

        # 'order': {
        #    'Subscription':{
        #        'op': 'subscribe',
        #        'args': ["orderBookL2:XBTUSD"],
        #    },
        #    'Header': ['IsSnapShot'] + OrderBookHeaderWithDepth(12),
        #    'FileName': 'BTC_USD-bitmex.book.csv',
        #    'RedisCollectKey': 'bitmex-BTC_USD-order_raw',
        #    'RedisOutputKey': 'bitmex-BTC_USD-order_processed',
        #    'DataHandler': 'process_order_data',
        #    'AmountMin': 1e-8,
        # },

        'orderbook10': {
            'Subscription': {
                'op': 'subscribe',
                'args': ["orderBook10:XBTUSD"],
            },
            'Header': order_book_header_with_depth(10),
            'FileName': 'BTC_USD-bitmex.book.csv',
            'RedisCollectKey': 'bitmex-BTC_USD-orderBook10_raw',
            'RedisOutputKey': 'bitmex-BTC_USD-orderBook10_processed',
            'DataHandler': 'process_order_book_10_data',
            'TickSize': 0.01,
            'AmountMin': 1e-8,
        },
    },
}

BinanceConfigs = {
    'BTC/USDT': {
        'order': {
            'url_append': '/ws/btcusdt@depth20',
            'Header': order_book_header_with_depth(20),
            'FileName': 'BTC_USDT-binance.book.csv',
            'RedisCollectKey': 'binance-BTC_USDT-order_raw',
            'RedisOutputKey': 'binance-BTC_USDT-order_processed',
            'DataHandler': 'process_order_data',
            'TickSize': 0.01,
            'AmountMin': 1e-8,
        },
        'ticker': {
            'url_append': '/ws/btcusdt@trade',
            'Header': [
                'size',
                'price',
                'eventtime',
                'tradeId',
                'buyerId',
                'sellerId',
            ],
            'FileName': 'BTC_USDT-binance.ticker.csv',
            'RedisCollectKey': 'binance-BTC_USDT-ticker_raw',
            'RedisOutputKey': 'binance-BTC_USDT-ticker_processed',
            'DataHandler': 'process_trade_data',
        },
    },
}

ExConfigs = {
    'Gdax': [GdaxConfigs, 'wss://ws-feed.gdax.com'],
    'Huobipro': [HuobiConfigs, 'wss://api.huobipro.com/ws'],
    'Gemini': [GeminiConfigs, 'wss://api.gemini.com/v1/'],
    'Bitmex': [BitmexConfigs, 'wss://www.bitmex.com/realtime'],
    'Binance': [BinanceConfigs, 'wss://stream.binance.com:9443'],
}
