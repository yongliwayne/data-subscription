# -*- coding: UTF-8 -*-
import unittest
import time
import datetime
import os
import smtplib
from email.mime.text import MIMEText
from ccws.configs import HOME_PATH
import logging


my_sender = 'report_ccws_2018@163.com'
my_pass = 'report123'
my_user = ['yongliwang2014@gmail.com', '1400012716@pku.edu.cn']


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


if __name__ == '__main__':
    test_dir = '%s/testcase/data-subscription/' % os.path.expanduser('~')

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
    logger = logging.getLogger('testcase-1')
    logger.setLevel(logging.INFO)
    if not os.path.exists("%s/log/" % HOME_PATH):
        os.mkdir("%s/log/" % HOME_PATH)
    handler = logging.FileHandler("%s/log/testcase-1.log" % HOME_PATH)
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    if fail_num != 0 or error_num != 0:
        send_report(message)
    else:
        logger.info('success')

