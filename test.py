# coding=utf-8

import ccws
from ccws.configs import load_logger_config

e=ccws.Gemini()
#print (e.ExConfig)
e.set_market('BTC/USD','trade')
#e.collect_data()
#e.process()
#e.process_data()
e.write_data_csv()

