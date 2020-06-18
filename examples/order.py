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
ibConn.connect(clientId=101, host="localhost", port=7497)

# # create a contract
# stk_contract = ibConn.createCFDContract("AAPL")

contract = ibConn.createFuturesContract("ES", exchange="GLOBEX", expiry="202006")


# create an order
order = ibConn.createStopOrder(quantity=-1,price=361.0,stop=360.,stop_limit=True) # use price=X for LMT orders

# submit an order (returns order id)
orderId = ibConn.placeOrder(contract, order)

print(orderId)

# let order fill
time.sleep(3)

# see the positions
print("Positions")
print(ibConn.positions)

# disconnect
ibConn.disconnect()
