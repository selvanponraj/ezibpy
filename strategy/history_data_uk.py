#!/usr/bin/env python
# -*- coding: UTF-8 -*-
#
# ezIBpy: a Pythonic Client for Interactive Brokers API
# https://github.com/ranaroussi/ezibpy
#
# Copyright 2015 Ran Aroussi
#
# Licensed under the GNU Lesser General Public License, v3.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.gnu.org/licenses/lgpl-3.0.en.html
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import ezibpy
import time

from datetime import timedelta
import datetime
from pytz import timezone

import shutil
import os
import csv

import os

# create a contract
# contract = ibConn.createStockContract("AAPL")

def is_non_zero_file(fpath):
    return os.path.isfile(fpath) and os.path.getsize(fpath) > 0

def get_history(source='orb_us_stocks',currency="USD", exchange="SMART"):
    contracts = []

    with open('../resources/' + source + '.csv') as file:
        data_dict = dict(filter(None, csv.reader(file)))
        selected_stocks = data_dict.keys()
    count = 0
    for symbol in selected_stocks:
        # if count == 50:
        #     ibConn.disconnect()
        #     ibConn.connect(clientId=101, host="localhost", port=7497)
        #     count = 0
        fpath = dirpath + symbol.replace(' ', '_') + '.csv'
        if os.path.isfile(fpath):
            try:
                os.remove(fpath)
            except OSError:
                pass

        contract = ibConn.createStockContract(symbol=symbol, currency=currency, exchange=exchange)

        ibConn.requestHistoricalData(contract, resolution="15 mins", lookback="1 D", csv_path=dirpath,
                                     end_datetime=algo_end_time)
        run = True
        count = 0
        while run:
            if is_non_zero_file(fpath):
                ibConn.cancelHistoricalData(contract)
                # count = count + 1
                run = False
            # else:
            #     count = count + 1
            #     if count == 50:
            #         run = False

    # # wait until stopped using Ctrl-c
    # try:
    #     while True:
    #         time.sleep(1)
    #         count = len([name for name in os.listdir(dirpath) if not name.startswith(".")])
    #         if count >= len(selected_stocks):
    #             print('Historical Data Fetched Count : ' + str(count))
    #             ibConn.cancelHistoricalData()
    #             ibConn.disconnect()
    #             exit(2)
    # except (KeyboardInterrupt, SystemExit):
    #     # cancel request & disconnect
    #     ibConn.cancelHistoricalData()
    #     ibConn.disconnect()


if __name__ == '__main__':

    api_time_format = '%Y%m%d %H:%M:%S'
    algo_time = timezone('UTC').localize(datetime.datetime.today() - timedelta(days=0))

    # initialize ezIBpy
    ibConn = ezibpy.ezIBpy()
    ibConn.connect(clientId=101, host="localhost", port=7497)

    source = 'orb_uk_stocks'
    dirpath = './../scan_results/' + source.split('_')[1] + '/'
    print(dirpath)
    # shutil.rmtree(dirpath)
    # os.mkdir(dirpath)
    algo_end_time = algo_time.replace(hour=15).replace(minute=45).replace(second=00).strftime(api_time_format)
    get_history('orb_uk_stocks','GBP','LSE')