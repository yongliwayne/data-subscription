import csv
import os
import datetime
import time
from ccws.configs import HOME_PATH
from ccws.configs import TIMEZONE
from ccws.configs import ExConfigs


def create_tomorrow_folder(path=''):
    tmr = datetime.datetime.fromtimestamp(time.time(), TIMEZONE)+datetime.timedelta(days=1)
    file_path = '%s/%4d/%02d/%02d' % (path, tmr.year, tmr.month, tmr.day)
    if not os.path.exists(file_path):
        os.makedirs(file_path)
    return file_path


def create_files():
    file_folder = create_tomorrow_folder(HOME_PATH)
    for exchange, currencies in ExConfigs.items():
        for currency, modes in currencies[0].items():
            for mode in modes.values():
                fn = mode['FileName']
                file = '%s/%s' % (file_folder, fn)
                if not os.path.exists(file):
                    with open(file, 'w') as csvFile:
                        csv_writer = csv.writer(csvFile)
                        csv_writer.writerow(['reporttimestamp', 'timestamp', 'datetime'] + mode['Header'])


if __name__ == '__main__':
    create_files()
