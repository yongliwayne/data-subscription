import pytz
from os.path import expanduser
import os

TIMEZONE = pytz.timezone('Asia/Shanghai')
# RedisHost = '192.168.118.130'
REDIS_HOST = 'localhost'

if os.path.exists('/data/'):
    HOME_PATH = '/data/data/subscription/cryptocurrency/'
else:
    HOME_PATH = '%s/data/subscription/cryptocurrency' % expanduser('~')

REDIS_CACHE_LENGTH = 100        # leave a buffer to debug, in case connection stop

ORDER_BOOK_DEPTH = 12
