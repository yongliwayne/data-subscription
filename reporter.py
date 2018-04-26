# -*- coding: UTF-8 -*-
from HTMLTestRunner import HTMLTestRunner
import unittest
import time
import os
import smtplib
from email.mime.text import MIMEText
from email.utils import formataddr

my_sender = 'applezjm123456@126.com'
my_pass = 'applezjm123'
my_user = 'applezjm123456@126.com'


def SendReport(file_new):
    with open(file_new, 'rb') as f:
        mail_body = f.read()
    msg = MIMEText(mail_body, 'html', 'utf-8')
    msg['From'] = formataddr(["sender", my_sender])
    msg['To'] = formataddr(["user", my_user])
    msg['Subject'] = "report in testcase-1"

    server = smtplib.SMTP_SSL("smtp.126.com", 465)
    server.login(my_sender, my_pass)
    server.sendmail(my_sender, [my_user, ], msg.as_string())
    server.quit()


def NewReport(TestReport):
    lists = os.listdir(TestReport)
    lists2 = sorted(lists)
    file_new = os.path.join(TestReport, lists2[-1])
    print(file_new)
    return file_new


if __name__ == '__main__':
    test_dir = '/home/data/testcase/data-subscription/'
    test_report = '/home/data/testcase/report/'

    discover = unittest.defaultTestLoader.discover(test_dir, pattern='test_*.py')

    now = time.strftime('%Y%m%d %H%M%S')
    filename = test_report + '\\' + now + 'result.html'
    fp = open(filename, 'wb')
    runner = HTMLTestRunner(stream=fp, title='测试报告', description='用例执行情况：')
    runner.run(discover)
    fp.close()

    new_report = NewReport(test_report)
    SendReport(new_report)

