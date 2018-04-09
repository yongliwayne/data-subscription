import ccws

ex = ccws.Gemini()
ex.set_market('BTC/USD', 'order')
ex.connect_redis()

fn = open('input.txt', 'w')
i = -1
while True:
    fn.writelines(ex.RedisConnection.lrange("gemini-BTC_USD_raw", i, i)[0].decode('utf-8'))
    fn.write('\n')
    i -= 1
