# coding=utf-8

import subprocess
from ccws.configs import ExConfigs
import time
from ccws.test.test_base import Test


def main():
    stop_str = 'bash test_killproc.sh %s %s %s %s'
    run_str = 'python test_data.py -e %s -s %s -m %s -f %s &'
    for func in ['write_into_redis', 'process_data', 'write_into_csv']:
        for exchange, values in ExConfigs.items():
            for currency, modes in values[0].items():
                for mode in modes.keys():
                    if exchange == 'Gemini' and mode == 'order':
                        subprocess.call(run_str % (exchange, currency, mode, func), shell=True)

    time.sleep(150)

    for func in ['write_into_redis', 'process_data', 'write_into_csv']:
        for exchange, values in ExConfigs.items():
            for currency, modes in values[0].items():
                for mode in modes.keys():
                    if exchange == 'Gemini' and mode == 'order':
                        subprocess.call(stop_str % (exchange, currency, mode, func), shell=True)

    for exchange, values in ExConfigs.items():
        for currency, modes in values[0].items():
            for mode in modes.keys():
                if exchange == 'Gemini' and mode == 'order':
                    te = Test(exchange, currency, mode)
                    te.compare_two_csv()


if __name__ == '__main__':
    main()
