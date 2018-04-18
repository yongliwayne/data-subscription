import pytz
import os

TIMEZONE = pytz.timezone('Asia/Shanghai')
REDIS_HOST = 'localhost'

if os.path.exists('/data/'):
    HOME_PATH = '/data/data/subscription/cryptocurrency/'
else:
    HOME_PATH = '%s/data/subscription/cryptocurrency' % os.path.expanduser('~')


