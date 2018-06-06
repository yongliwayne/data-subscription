# coding=utf-8

import json
import time
from ccws import Exchange


class Binance(Exchange):
    ExchangeId = 'Binance'

    def collect_data(self):
        self.connect_redis()
        self.run_websocketapp(
            url_append=self.Config['url_append']
        )
