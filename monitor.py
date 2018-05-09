# -*- coding: UTF-8 -*-
import unittest
import time
import datetime
import os
import smtplib
from email.mime.text import MIMEText
import logging
from logging.handlers import RotatingFileHandler
import argparse


my_sender = 'report_ccws_2018@163.com'
my_pass = 'report123'
my_user = ['yongliwang2014@gmail.com', '1400012716@pku.edu.cn']
logger = logging.getLogger('testcase')


def set_logger(path):
    logger.setLevel(logging.INFO)
    if not os.path.exists("%s/log/" % path):
        os.mkdir("%s/log/" % path)
    handler = RotatingFileHandler("%s/log/testcase.log" % path)
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def send_report(mail_body):
    msg = MIMEText(mail_body, 'plain', 'utf-8')
    msg['From'] = my_sender
    msg['To'] = ', '.join(my_user)
    tmr = datetime.datetime.fromtimestamp(time.time())
    msg['Subject'] = 'CCWS Test Report %4d/%02d/%02d' % (tmr.year, tmr.month, tmr.day)

    server = smtplib.SMTP_SSL("smtp.163.com", 465)
    server.login(my_sender, my_pass)
    server.sendmail(my_sender, my_user, msg.as_string())
    server.quit()


def run_test():
    test_dir = '/data/data/script/data-subscription/'
    discover = unittest.defaultTestLoader.discover(test_dir, pattern='test_*.py')
    runner = unittest.TextTestRunner()
    res = runner.run(discover)
    all_num = res.testsRun
    fail_num = len(res.failures)
    error_num = len(res.errors)
    failure = res.failures
    error = res.errors
    message = """
    All case number
    ...................
    %d
    Failed case number
    ...................
    %d
    Failed case and reason
    """ % (all_num, fail_num)
    for i in failure:
        message_add = """
        .......................
        %s
        """ % str(i)
        message += message_add
    message += """
    Error case and reason
    ........................
    """
    for i in error:
        message_add = """
        .......................
        %s
        """ % str(i)
        message += message_add
    logger.info(message)
    if fail_num != 0 or error_num != 0:
        send_report(message)


def check_process(path, time_gap):
    today = datetime.datetime.fromtimestamp(time.time())
    base_dir = '%s/%4d/%02d/%02d/' % (path, today.year, today.month, today.day)
    file_list = os.listdir(base_dir)
    wrong_info = ''

    for i in range(len(file_list)):
        path = os.path.join(base_dir, file_list[i])
        modify_time = os.path.getmtime(path)
        now = time.time()
        if abs(now - modify_time) > time_gap:
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


def main():
    parser = argparse.ArgumentParser(description="Monitor.")
    parser.add_argument('-m', '--mode', metavar='mode', required=True, help='function mode.')
    parser.add_argument('-p', '--path', metavar='path', required=True, default='', help='path.')
    parser.add_argument('-t', '--timegap', metavar='timegap', required=False, default=0, help='timegap.')
    args = parser.parse_args()

    mode, path, time_gap = args.mode, args.path, float(args.timegap)

    if mode == 'run_test':
        set_logger(path)
        run_test()
    elif mode == 'check_process':
        set_logger(path)
        check_process(path, time_gap)


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        logger.info(e)
