import pytz
from os.path import expanduser
import os

TIMEZONE = pytz.timezone('Asia/Shanghai')
REDIS_HOST = '192.168.118.130'

if os.path.exists('/data/'):
    HOME_PATH = '/data/data/'
else:
    HOME_PATH = '%s/data/' % expanduser('~')

REDIS_CACHE_LENGTH = 100        # leave a buffer to debug, in case connection stop

ORDER_BOOK_DEPTH = 12
