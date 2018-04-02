#!/usr/bin/env python3
# coding=utf-8

import datetime
import time
import subprocess
from ccws.configs import HOME_PATH
from ccws.configs import TIMEZONE


def main():
    yesterday = datetime.datetime.fromtimestamp(time.time(), TIMEZONE)+datetime.timedelta(days=-1)
    file_path = '%s/%4d/%02d/%02d' % (HOME_PATH, yesterday.year, yesterday.month, yesterday.day)
    subprocess.call('gzip %s/*' % file_path, shell=True)


if __name__ == '__main__':
    main()
