#!/usr/bin/env python3
# coding=utf-8

import datetime
import time
import subprocess
from ccws.configs import TIMEZONE

HomePaths = ['/data/data/subscription/cryptocurrency', '/data/data/subscription/cryptocurrency-future']


def main():
    for hp in HomePaths:
        yesterday = datetime.datetime.fromtimestamp(time.time(), TIMEZONE)+datetime.timedelta(days=-1)
        file_path = '%s/%4d/%02d/%02d' % (hp, yesterday.year, yesterday.month, yesterday.day)
        subprocess.call('ssh data@172.24.119.248 "mkdir -p %s"' % file_path, shell=True)
        subprocess.call('gzip %s/*' % file_path, shell=True)
        subprocess.call('scp -r %s/* data@172.24.119.248:%s' % (file_path, file_path), shell=True)


if __name__ == '__main__':
    main()
