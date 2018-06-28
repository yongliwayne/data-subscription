# coding=utf-8

import json
from ccws import Exchange


class Bitfinex(Exchange):
    ExchangeId = 'Bitfinex'

    def collect_data(self):
        self.connect_redis()
        self.run_websocketapp(
            on_open=self.on_open,
        )

    def on_open(self, ws):
        ws.send(json.dumps(self.Config['Subscription']))
        self.Logger.info('Subscription')
