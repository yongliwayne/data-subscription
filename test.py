# coding=utf-8

import ccws
e=ccws.Bitmex()
#print (e.ExConfig)
e.set_market('BTC/USD', 'trade')
e.collect_data()
#e.process()
e.process_data()
#e.write_data_csv()
