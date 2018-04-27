# coding:utf-8

import os
import datetime
import time
from ccws.configs import HOME_PATH
from reporter import send_report

tmr = datetime.datetime.fromtimestamp(time.time())
base_dir = '%s/%4d/%02d/%02d/' % (HOME_PATH, tmr.year, tmr.month, tmr.day)
file_list = os.listdir(base_dir)
wrong_info = ''

for i in range(len(file_list)):
    path = os.path.join(base_dir, file_list[i])
    modify_time = os.path.getmtime(path)
    now = time.time()
    if abs(now - modify_time) > 60:
        modify_date = datetime.datetime.fromtimestamp(modify_time).strftime('%Y-%m-%d %H:%M:%S')
        now_date = datetime.datetime.fromtimestamp(now).strftime('%Y-%m-%d %H:%M:%S')
        wrong_info += """
        the csv file:%s
        last modified time:%s
        check time:%s
        -------------------------------------------
        """ % (file_list[i], modify_date, now_date)
if wrong_info != '':
    send_report(wrong_info)
