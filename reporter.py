# -*- coding: UTF-8 -*-
import unittest
import time
import datetime
import os
import smtplib
from email.mime.text import MIMEText
from email.utils import COMMASPACE


my_sender = 'applezjm123456@126.com'
my_pass = 'applezjm123'
my_user = ['yongliwang2014@gmail.com', '1400012716@pku.edu.cn']


def SendReport(mail_body):
    msg = MIMEText(mail_body, 'plain', 'utf-8')
    msg['From'] = my_sender
    msg['To'] = COMMASPACE.join(my_user)
    tmr = datetime.datetime.fromtimestamp(time.time())
    msg['Subject'] = 'report in testcase-1 %4d/%02d/%02d' % (tmr.year, tmr.month, tmr.day)

    server = smtplib.SMTP_SSL("smtp.126.com", 465)
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
    reason = res.failures
    message = """
    All case number
    ...................
    %d
    Failed case number
    ...................
    %d
    Failed case and reason
    """ % (all_num, fail_num)
    for i in reason:
        message_add = """
        .......................
        %s
        """ % str(i)
        message += message_add
    if len(res.failures) != 0:
        SendReport(message)

