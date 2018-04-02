# coding=utf-8

import subprocess
from ccws.configs import ExConfigs


def main():
    stop_str = 'bash killproc.sh %s %s %s %s'
    run_str = 'python3 data.py -e %s -s %s -m %s -f %s &'
    for func in ['collect_data', 'process_data', 'write_data_csv']:
        for exchange, values in ExConfigs.items():
            for currency, modes in values[0].items():
                for mode in modes.keys():
                    subprocess.call(stop_str % (exchange, currency, mode, func), shell=True)
                    subprocess.call(run_str % (exchange, currency, mode, func), shell=True)


if __name__ == '__main__':
    main()
