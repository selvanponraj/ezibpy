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

api_time_format = '%Y%m%d %H:%M:%S'
algo_time = timezone('UTC').localize(datetime.datetime.today() - timedelta(days=0))
algo_end_time = algo_time.replace(hour=9).replace(minute=15).replace(second=00).strftime(api_time_format)


dirpath='./../scan_results/us/'
shutil.rmtree(dirpath)
os.mkdir(dirpath)

# initialize ezIBpy
ibConn = ezibpy.ezIBpy()
ibConn.connect(clientId=100, host="localhost", port=7497)

# create a contract
# contract = ibConn.createStockContract("AAPL")

contracts =[]
source = 'orb_uk_stocks'
with open('../resources/' + source + '.csv') as file:
    data_dict = dict(filter(None, csv.reader(file)))
    selected_stocks = data_dict.keys()

for symbol in selected_stocks:
     contracts.append(ibConn.createStockContract(symbol=symbol, currency="GBP", exchange="LSE"))

ibConn.requestHistoricalData(contracts=contracts, resolution="15 mins", lookback="1 D", csv_path=dirpath, end_datetime=algo_end_time)

# wait until stopped using Ctrl-c
try:
    while True:
        time.sleep(1)
        import os
        count = len([name for name in os.listdir(dirpath) if not name.startswith(".")])
        if count >= len(selected_stocks):
            print('Historical Data Fetched')
            ibConn.cancelHistoricalData()
            ibConn.disconnect()
            exit(2)
except (KeyboardInterrupt, SystemExit):
    # cancel request & disconnect
    ibConn.cancelHistoricalData()
    ibConn.disconnect()