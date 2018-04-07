# coding=utf-8
from ccws.configs.constants import ORDER_BOOK_DEPTH

OrderBookHeaderWithDepth = sum([['bidp%d' % i, 'bidv%d' % i] for i in range(ORDER_BOOK_DEPTH)], []) \
                           + sum([['askp%d' % i, 'askv%d' % i] for i in range(ORDER_BOOK_DEPTH)], [])

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
            'Header': OrderBookHeaderWithDepth,
            'FileName': 'BTC_USDT-huobipro.book.csv',
            'RedisCollectKey': 'huobipro-BTC_USDT-order_raw',
            'RedisOutputKey': 'huobipro-BTC_USDT-order_processed',
            'DataHandler': 'process_order_book_data',
        },
    },
}


GdaxConfigs = {
    'BTC/USD': {
        # 'order.received': {
        #     'Subscription': {
        #         'type': 'subscribe',
        #         'channels': [{'name': 'full', 'product_ids': ['BTC-USD']}],
        #     },
        #     'Header': [
        #         'client_oid',
        #         'order_id',
        #         'order_type',
        #         'price',
        #         'sequence',
        #         'side',
        #         'size',
        #         'funds',
        #     ],
        #     'FileName': 'BTC_USD-gdax.order.received.csv',
        #     'RedisQueueKey': 'gdax-BTC_USD-order_received',
        # },
        #
        # 'order.open': {
        #     'Subscription': {
        #         'type': 'subscribe',
        #         'channels': [{'name': 'full', 'product_ids': ['BTC-USD']}],
        #     },
        #     'Header': [
        #         'order_id',
        #         'price',
        #         'remaining_size',
        #         'sequence',
        #         'side',
        #     ],
        #     'FileName': 'BTC_USD-gdax.order.open.csv',
        #     'RedisQueueKey': 'gdax-BTC_USD-order_open',
        # },
        #
        # 'order.done': {
        #     'Subscription': {
        #         'type': 'subscribe',
        #         'channels': [{'name': 'full', 'product_ids': ['BTC-USD']}],
        #     },
        #     'Header': [
        #         'order_id',
        #         'price',
        #         'reason',
        #         'remaining_size',
        #         'sequence',
        #         'side',
        #     ],
        #     'FileName': 'BTC_USD-gdax.order.done.csv',
        #     'RedisQueueKey': 'gdax-BTC_USD-order_done',
        # },

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
            'Header': ['IsSnapShot'] + OrderBookHeaderWithDepth,
            'FileName': 'BTC_USD-gdax.book.csv',
            'RedisCollectKey': 'gdax-BTC_USD-order_raw',
            'RedisOutputKey': 'gdax-BTC_USD-order_processed',
            'DataHandler': 'process_order_book_data',
            'TickSize': 0.01,
            'AmountMin': 1e-8,
        },

    },
}
GeminiConfigs={
    'BTC/USD':{
        'order':{
            'url_append': 'v1/marketdata/BTCUSD',
            'Header': ['IsSnapShot'] + OrderBookHeaderWithDepth,
            'FileName': 'BTC_USD-gemini.book.csv',
            'RedisCollectKey': 'gemini-BTC_USD_raw',
            'RedisOutputKey': 'gemini-BTC_USD_processed',
            'DataHandler': 'process_order_book_data',
            'TickSize': 0.01,
            'AmountMin': 1e-8,
        },
    },
}
ExConfigs = {
   'Gdax': [GdaxConfigs, 'wss://ws-feed.gdax.com'],
   'Huobipro': [HuobiConfigs, 'wss://api.huobipro.com/ws'],
   'Gemini': [GeminiConfigs, 'wss://api.gemini.com/'],
}
