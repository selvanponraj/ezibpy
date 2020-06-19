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

# initialize ezIBpy
ibConn = ezibpy.ezIBpy()
ibConn.connect(clientId=101, host="localhost", port=7496)

# # create a contract


# contract = ibConn.createFuturesContract("ES", exchange="GLOBEX", expiry="202006")

contract = ibConn.createCFDContract("JMAT",'GBP')

# create an order
order = ibConn.createStopOrder(quantity=118,price=2133.80,stop=2133.80,stop_limit=True) # use price=X for LMT orders
# submit an order (returns order id)
orderId = ibConn.placeOrder(contract, order)

order = ibConn.createStopOrder(quantity=-118,price=2099.00,stop=2099.00,stop_limit=True)
orderId = ibConn.placeOrder(contract, order)

print(orderId)

# let order fill
time.sleep(3)

# see the positions
print("Positions")
print(ibConn.positions)

# disconnect
ibConn.disconnect()
