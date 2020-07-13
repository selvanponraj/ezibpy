from datetime import timedelta
import datetime

import requests_cache
from pytz import timezone
import ezibpy
import time
from trading_ig import IGService
from trading_ig.config import config

import csv
import pandas as pd
import logging

import os

# after ezibpy is imported, we can silence error logging
# logger = logging.getLogger('ezibpy').setLevel(logging.CRITICAL)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class Orb:
    def __init__(self):
        logging.basicConfig(level=logging.INFO)
        expire_after = timedelta(hours=1)

        self.session = requests_cache.CachedSession(
            cache_name="cache", backend="sqlite", expire_after=expire_after
        )

        # set expire_after=None if you don't want cache expiration
        # set expire_after=0 if you don't want to cache queries

        # config = IGServiceConfig()

        # no cache
        self.ig_service = IGService(
            config.username, config.password, config.api_key, config.acc_type
        )

        # if you want to globally cache queries
        # ig_service = IGService(config.username, config.password, config.api_key, config.acc_type, session)

        self.ig_service.create_session()

        # self.accounts = self.ig_service.fetch_accounts()
        # print("accounts:\n%s" % self.accounts)

    def get_qunatity(self, orbs_df, risk):
        shares = {}
        for _, row in orbs_df.iterrows():
            shares[row['symbol']] = int(
                (risk / ((row['high'] + row['low']) / 2)) * 100
            )
        return shares

    def get_ig_sb_qunatity(self, orbs_df, risk):
        try:
            shares = {}
            for _, row in orbs_df.iterrows():
                avg_price = (row['high'] + row['low']) / 2
                margin_required = avg_price * 0.20
                qty = round(float(risk / margin_required), 1)
                if qty < 0.5:
                    qty = 0.5
                shares[row['epic']] = qty
            return shares
        except:
            logger.error("Unable to get Quantity, Verify Scan Result has both high and low price columns")
            exit(2)

    def ig_scanners(self, strategy, source='orb_uk_stocks', timeframe='15Min'):
        source = source + '_ig'
        orbs = pd.DataFrame(columns=['symbol', 'high', 'low', 'edge', 'diff(%)', 'qty', 'risk', 'tp1', 'tp2', 'epic'])

        data_dict = {}
        if strategy == 'orb':
            window_size = 4
            with open('../resources/' + source + '.csv') as file:
                data_dict = dict(filter(None, csv.reader(file)))
                selected_stocks = data_dict.keys()
        else:
            window_size = 3
            with open('../resources/' + source + '.csv') as file:
                selected_stocks = [line.split(',')[0].strip() for line in file]

        (start_date, end_date) = (algo_start_time, algo_end_time)

        for stock in selected_stocks:
            words = stock.split('-')
            symbol = words[0]
            epic = words[1]
            resolution = timeframe
            try:
                response = self.ig_service.fetch_historical_prices_by_epic_and_date_range(epic, resolution, start_date,
                                                                                          end_date)
            except:
                logger.error("Historical Data Not found- Try  after 8:30AM UK time")
                exit(2)
            bars = response['prices']['last']

            if strategy == 'orb' and len(bars) > 4:
                open_candle = bars.iloc[0]
                second_candle = bars.iloc[1]
                third_candle = bars.iloc[2]
                fourth_candle = bars.iloc[3]
                bars_total_volume = bars['Volume'].sum()
                if ((second_candle['High'] < open_candle['High'] and second_candle['Low'] > open_candle['Low']) and
                        (third_candle['High'] < open_candle['High'] and third_candle['Low'] > open_candle['Low']) and
                        (fourth_candle['High'] < open_candle['High'] and fourth_candle['Low'] > open_candle['Low'])):

                    # print(symbol)
                    # print(bars)

                    high = round(open_candle['High'], 2)
                    low = round(open_candle['Low'], 2)
                    cp = ((high - low) / ((high + low) / 2)) * 100
                    qty = int(capital / ((high + low) / 2))
                    if qty < 0.5:
                        qty = 0.5
                    risk = round((high - low) * qty)
                    tp1 = round(risk * 1.5)
                    tp2 = round(risk * 2)

                    orbs = orbs.append({
                        'symbol': symbol,
                        'epic': epic.replace('CASH', 'DAILY'),
                        'high': round(open_candle['High'], 2),
                        'low': round(open_candle['Low'], 2),
                        'edge': float(data_dict[stock]),
                        'diff(%)': round(cp, 2),
                        'qty': qty,
                        'risk': risk,
                        'tp1': tp1,
                        'tp2': tp2
                    }, ignore_index=True)
            elif strategy == '10am-buy' and len(bars) > 3:
                open_candle = bars.iloc[0]
                second_candle = bars.iloc[1]
                third_candle = bars.iloc[2]
                bars_total_volume = bars['Volume'].sum()
                if ((second_candle['High'] < open_candle['High'] and second_candle['Low'] > open_candle['Low']) and
                                third_candle['Close'] > open_candle['High']):
                    # print(symbol)
                    # print(bars)

                    high = round(open_candle['High'], 2)
                    low = round(open_candle['Low'], 2)
                    cp = ((high - low) / ((high + low) / 2)) * 100
                    qty = int(capital / ((high + low) / 2))
                    if qty < 0.5:
                        qty = 0.5
                    risk = round((high - low) * qty)
                    tp1 = round(risk * 1.5)
                    tp2 = round(risk * 2)

                    orbs = orbs.append({
                        'symbol': symbol,
                        'epic': epic.replace('CASH', 'DAILY'),
                        'high': round(open_candle['High'], 2),
                        'low': round(open_candle['Low'], 2),
                        'edge': bars_total_volume,
                        'diff(%)': round(cp, 2),
                        'qty': qty,
                        'risk': risk,
                        'tp1': tp1,
                        'tp2': tp2
                    }, ignore_index=True)
            elif strategy == '10am-sell' and len(bars) > 3:
                open_candle = bars.iloc[0]
                second_candle = bars.iloc[1]
                third_candle = bars.iloc[2]
                bars_total_volume = bars['Volume'].sum()

                # print((second_candle['High'] < open_candle['High'] and second_candle['Low']  > open_candle['Low'] ))
                # print((third_candle['Low']  < open_candle['Low']  and third_candle['Close'] < open_candle['Close']))

                if ((second_candle['High'] < open_candle['High'] and second_candle['Low'] > open_candle['Low']) and
                         third_candle['Close'] < open_candle['Low']):
                    # print(symbol)
                    # print(bars)

                    high = round(open_candle['High'], 2)
                    low = round(open_candle['Low'], 2)
                    cp = ((high - low) / ((high + low) / 2)) * 100
                    qty = int(capital / ((high + low) / 2))
                    if qty < 0.5:
                        qty = 0.5
                    risk = round((high - low) * qty)
                    tp1 = round(risk * 1.5)
                    tp2 = round(risk * 2)

                    orbs = orbs.append({
                        'symbol': symbol,
                        'epic': epic.replace('CASH', 'DAILY'),
                        'high': round(open_candle['High'], 2),
                        'low': round(open_candle['Low'], 2),
                        'edge': bars_total_volume,
                        'diff(%)': round(cp, 2),
                        'qty': qty,
                        'risk': risk,
                        'tp1': tp1,
                        'tp2': tp2
                    }, ignore_index=True)
        orbs = orbs.sort_values(['edge'], ascending=[False])
        if not orbs.empty:
            open('../scan_results/uk_' + strategy + r'_ig_result.csv', 'w').close()
            orbs.to_csv('../scan_results/uk_' + strategy + r'_ig_result.csv', header=True, index=None, sep=',')
        return orbs;

    def ib_scanners(self, strategy, source='orb_uk_stocks', timeframe='15Min'):

        orbs = pd.DataFrame(columns=['symbol', 'high', 'low', 'edge', 'diff(%)', 'qty', 'risk', 'tp1', 'tp2'])
        bar_time_format = '%Y-%m-%d %H:%M:%S.%f'
        data_dict = {}

        with open('../resources/' + source + '.csv') as file:
            data_dict = dict(filter(None, csv.reader(file)))
            selected_stocks = data_dict.keys()

        for symbol in selected_stocks:
            symbol = symbol.replace(" ", '_')
            if not (os.path.isfile(dirpath + symbol + '.csv')):
                print('Symbol not found : ', symbol)
                continue

            bars = pd.read_csv(dirpath + symbol + '.csv')
            bars = bars.set_index(pd.DatetimeIndex(bars['datetime']))
            if strategy == 'orb':
                bar_start_time = algo_time.replace(hour=7).replace(minute=59).replace(second=59).strftime(
                    bar_time_format)
                bar_end_time = algo_time.replace(hour=9).replace(minute=00).replace(second=00).strftime(
                    bar_time_format)

                bars = bars[bars.index >= bar_start_time]
                bars = bars[bars.index <= bar_end_time]

                if len(bars) > 4:

                    open_candle = bars.iloc[0]
                    second_candle = bars.iloc[1]
                    third_candle = bars.iloc[2]
                    fourth_candle = bars.iloc[3]

                    if ((second_candle.H < open_candle.H and second_candle.L > open_candle.L) and
                            (third_candle.H < open_candle.H and third_candle.L > open_candle.L) and
                            (fourth_candle.H < open_candle.H and fourth_candle.L > open_candle.L)):
                        # print(symbol)
                        # print(bars)

                        high = round(open_candle.H, 2)
                        low = round(open_candle.L, 2)
                        cp = ((high - low) / ((high + low) / 2)) * 100
                        qty = int(capital / (((high + low) / 2) / 100))
                        risk = round((high - low) * (qty / 100))
                        tp1 = round(risk * 1.5)
                        tp2 = round(risk * 2)

                        orbs = orbs.append({
                            'symbol': symbol,
                            'high': high,
                            'low': low,
                            'edge': float(data_dict[symbol.replace('_', ' ')]),
                            'diff(%)': round(cp, 2),
                            'qty': qty,
                            'risk': risk,
                            'tp1': tp1,
                            'tp2': tp2
                        }, ignore_index=True)
            elif strategy == '10am-buy':
                bar_start_time = algo_time.replace(hour=7).replace(minute=59).replace(second=59).strftime(
                    bar_time_format)
                bar_end_time = algo_time.replace(hour=9).replace(minute=00).replace(second=00).strftime(
                    bar_time_format)
                bars = bars[bars.index >= bar_start_time]
                bars = bars[bars.index <= bar_end_time]
                if len(bars) > 3:
                    open_candle = bars.iloc[0]
                    second_candle = bars.iloc[1]
                    third_candle = bars.iloc[2]
                    bars_total_volume = bars['V'].sum()
                    if ((second_candle.H < open_candle.H and second_candle.L > open_candle.L) and
                            third_candle.C > open_candle.H):
                        print(symbol)
                        print(bars[['H', 'L', 'C']])

                        high = round(third_candle.H, 2)
                        low = round(third_candle.L, 2)
                        cp = ((high - low) / ((high + low) / 2)) * 100
                        qty = int(capital / (((high + low) / 2) / 100))
                        risk = round((high - low) * (qty / 100))
                        tp1 = round(risk * 1.5)
                        tp2 = round(risk * 2)

                        orbs = orbs.append({
                            'symbol': symbol,
                            'high': high,
                            'low': low,
                            'edge': bars_total_volume,
                            'diff(%)': round(cp, 2),
                            'qty': qty,
                            'risk': risk,
                            'tp1': tp1,
                            'tp2': tp2
                        }, ignore_index=True)

            elif strategy == '10am-sell':
                bar_start_time = algo_time.replace(hour=7).replace(minute=59).replace(second=59).strftime(
                    bar_time_format)
                bar_end_time = algo_time.replace(hour=9).replace(minute=00).replace(second=00).strftime(
                    bar_time_format)

                bars = bars[bars.index >= bar_start_time]
                bars = bars[bars.index <= bar_end_time]

                if len(bars) > 3:
                    open_candle = bars.iloc[0]
                    second_candle = bars.iloc[1]
                    third_candle = bars.iloc[2]
                    bars_total_volume = bars['V'].sum()

                    if ((second_candle.H < open_candle.H and second_candle.L > open_candle.L) and
                        third_candle.C < open_candle.L):
                        # print(symbol)
                        # print(bars)
                        high = round(third_candle.H, 2)
                        low = round(third_candle.L, 2)
                        cp = ((high - low) / ((high + low) / 2)) * 100
                        qty = int(capital / (((high + low) / 2) / 100))
                        risk = round((high - low) * (qty / 100))
                        tp1 = round(risk * 1.5)
                        tp2 = round(risk * 2)

                        orbs = orbs.append({
                            'symbol': symbol,
                            'high': high,
                            'low': low,
                            'edge': bars_total_volume,
                            'diff(%)': round(cp, 2),
                            'qty': qty,
                            'risk': risk,
                            'tp1': tp1,
                            'tp2': tp2
                        }, ignore_index=True)

        orbs = orbs.sort_values(['edge'], ascending=[False])

        if not orbs.empty:
            open('../scan_results/uk_' + strategy + r'_ib_result.csv', 'w').close()
            orbs.to_csv('../scan_results/uk_' + strategy + r'_ib_result.csv', header=True, index=None, sep=',')
        return orbs;

    def ib_place_orders(self, strategy, scan_results, quantity):
        for _, row in scan_results.iterrows():
            symbol = row['symbol']
            qty = row['qty']
            cfd_contract = ibConn.createCFDContract(symbol, 'GBP')
            high = row['high']
            low = row['low']
            if strategy == 'orb':
                user_input = input(str(symbol) + '-Sell - Would you like to place IB order (Yes/No)? ').upper()
                if user_input == 'YES':
                    # # create an stop order - sell
                    order = ibConn.createStopOrder(quantity=-qty, price=low, stop=low, stop_limit=True)
                    # submit an order (returns order id)
                    ibConn.placeOrder(cfd_contract, order)

                user_input = input(str(symbol) + '- Buy - Would you like to place IB order (Yes/No)? ').upper()
                if user_input == 'YES':
                    # # create an stop order - buy
                    order = ibConn.createStopOrder(quantity=qty, price=high, stop=high, stop_limit=True)
                    # submit an order (returns order id)
                    ibConn.placeOrder(cfd_contract, order)
            elif strategy == '10am-buy':
                user_input = input(str(symbol) + '- Buy - Would you like to place IB order (Yes/No)? ').upper()
                if user_input == 'YES':
                    order = ibConn.createBracketOrder(cfd_contract, quantity=qty, entry=high, stop=low)
            elif strategy == '10am-sell':
                user_input = input(str(symbol) + '- Sell - Would you like to place IB order (Yes/No)? ').upper()
                if user_input == 'YES':
                    order = ibConn.createBracketOrder(cfd_contract, quantity=-qty, entry=low, stop=high)
            # let order fill
            time.sleep(3)

    def ig_place_orders(self, strategy, scan_results, quantity):
        for _, row in scan_results.iterrows():
            epic = row['epic']
            qty = quantity[epic]
            risk = (row['high'] - row['low']) * qty
            print("Risk: " + epic + "-" + str(risk))
            if strategy == 'orb':
                user_input = input(epic + ' - BUY - Would you like to place order (Yes/No)? ').upper()
                if user_input == 'YES':
                    response = self.ig_service.create_working_order('GBP', 'BUY', epic, 'DFB', False, row['high'], qty,
                                                                    'GOOD_TILL_CANCELLED', 'STOP', None, None, None,
                                                                    None
                                                                    )
                    print(response)
                    if response['dealStatus'] == 'REJECTED':
                        response = self.ig_service.create_working_order('GBP', 'BUY', epic, 'DFB', False, row['high'],
                                                                        qty,
                                                                        'GOOD_TILL_CANCELLED', 'LIMIT', None, None,
                                                                        None,
                                                                        None)
                        print(response)
                        if response['dealStatus'] == 'REJECTED':
                            response = self.ig_service.create_open_position('GBP', 'BUY', epic, 'DFB', False, False,
                                                                            None, None, None, 'MARKET', None, qty, None,
                                                                            None, None, None)

                            print(response)

                else:
                    print("No Buy Order Placed for : " + epic)

                user_input = input(epic + ' - SELL - Would you like to place order (Yes/No)? ').upper()
                if user_input == 'YES':
                    response = self.ig_service.create_working_order('GBP', 'SELL', epic, 'DFB', False, row['low'], qty,
                                                                    'GOOD_TILL_CANCELLED', 'STOP', None, None, None,
                                                                    None)
                    print(response)
                    if response['dealStatus'] == 'REJECTED':
                        response = self.ig_service.create_working_order('GBP', 'SELL', epic, 'DFB', False, row['low'],
                                                                        qty, 'GOOD_TILL_CANCELLED', 'STOP', None, None,
                                                                        None, None)
                        print(response)
                        if response['dealStatus'] == 'REJECTED':
                            response = self.ig_service.create_open_position('GBP', 'SELL', epic, 'DFB', False, False,
                                                                            None, None, None, 'MARKET', None, qty, None,
                                                                            None, None, None)

                            print(response)
                else:
                    print("No Sell Order Placed for : " + epic)

            if strategy == '10am-buy':
                user_input = input(epic + ' - BUY - Would you like to place order (Yes/No)? ').upper()
                if user_input == 'YES':
                    response = self.ig_service.create_working_order('GBP', 'BUY', epic, 'DFB', False, row['high'], qty,
                                                                    'GOOD_TILL_CANCELLED', 'STOP', None, None, None,
                                                                    row['low'])
                    print(response)
                    if response['dealStatus'] == 'REJECTED':
                        response = self.ig_service.create_working_order('GBP', 'BUY', epic, 'DFB', False, row['high'],
                                                                        qty, 'GOOD_TILL_CANCELLED', 'LIMIT', None, None,
                                                                        None, row['low'])

                        print(response)
                        if response['dealStatus'] == 'REJECTED':
                            response = self.ig_service.create_open_position('GBP', 'BUY', epic, 'DFB', False, False,
                                                                            None, None, None, 'MARKET', None, qty, None,
                                                                            None, None, None)
                            print(response)
                else:
                    print("No BUY Order Placed for : " + epic)

            if strategy == '10am-sell':
                user_input = input(epic + ' - SELL - Would you like to place order (Yes/No)? ').upper()
                if user_input == 'YES':
                    response = self.ig_service.create_working_order('GBP', 'SELL', epic, 'DFB', False, row['low'], qty,
                                                                    'GOOD_TILL_DATE', 'STOP', None, None, None,
                                                                    row['high'],
                                                                    position_end_time)
                    print(response)
                    if response['dealStatus'] == 'REJECTED':
                        response = self.ig_service.create_working_order('GBP', 'SELL', epic, 'DFB', False, row['low'],
                                                                        qty, 'GOOD_TILL_DATE', 'LIMIT', None, None,
                                                                        None,
                                                                        row['high'], position_end_time)

                        print(response)
                        if response['dealStatus'] == 'REJECTED':
                            response = self.ig_service.create_open_position('GBP', 'SELL', epic, 'DFB', False, False,
                                                                            None, None, None, 'MARKET', None, qty, None,
                                                                            None, None, None)
                            print(response)
                else:
                    print("No SELL Order Placed for : " + epic)


if __name__ == '__main__':
    orb = Orb()
    api_time_format = '%Y-%m-%dT%H:%M:%S'
    algo_time = timezone('UTC').localize(datetime.datetime.today() - timedelta(days=2))
    algo_start_time = algo_time.replace(hour=8).replace(minute=00).replace(second=00).strftime(api_time_format)
    algo_end_time = algo_time.replace(hour=9).replace(minute=00).replace(second=00).strftime(api_time_format)
    # algo_time = timezone('UTC').localize(datetime.datetime.today() - timedelta(days=0))
    position_end_time = algo_time.replace(hour=9).replace(minute=00).replace(second=00).strftime(api_time_format)

    st_list = ['10am-buy', '10am-sell', 'orb']
    print("Available Strategies:")
    for i, strategy in enumerate(st_list, start=1):
        print('{}. {}'.format(i, strategy))

    while True:
        try:
            selected = int(input('Select a strategy (1-{}): '.format(i)))
            strategy = st_list[selected - 1]
            print('You have selected {}'.format(strategy))
            break
        except (ValueError, IndexError):
            print('This is not a valid selection. Please enter number between 1 and {}!'.format(i))

    # initialize ezIBpy
    ibConn = ezibpy.ezIBpy()
    ibConn.connect(clientId=100, host="localhost", port=7497)

    capital = 10000
    source = "orb_uk_stocks"
    dirpath = './../scan_results/' + source.split('_')[1] + '/'

    # scan_results = orb.ig_scanners(strategy, source)
    # print(strategy.upper() + " IB Scan Results:")
    # print(scan_results.to_string(index=False))

    user_input = input('Would you like run IG Scanner (Yes/No)? ').upper()
    if user_input == 'YES':
        ig_scan_results = orb.ig_scanners(strategy)
        print(strategy.upper() + " IG Scan Results:")
        print(ig_scan_results.to_string(index=False))

    user_input = input('Would you like run IB Scanner (Yes/No)? ').upper()
    if user_input == 'YES':
        scan_results = orb.ib_scanners(strategy, source)
        print(strategy.upper() + " IB Scan Results:")
        print(scan_results.to_string(index=False))
    #
    user_input = input('Would you like to place orders (Yes/No)? ').upper()
    if user_input == 'YES':
        print("Placing Orders ....")
        ########## IB ORDER ##############
        user_input = input('Would you like to place IB orders (Yes/No)? ').upper()
        if user_input == 'YES':
            scan_results = pd.read_csv('../scan_results/uk_' + strategy + r'_ib_result.csv')
            print(strategy.upper() + " IB Scan Results:")
            print(scan_results.to_string(index=False))
            qty = orb.get_qunatity(scan_results, 10000)
            print(qty)
            orb.ib_place_orders(strategy, scan_results.head(5), qty)

        user_input = input('Would you like to place IG orders (Yes/No)? ').upper()
        if user_input == 'YES':
            ########## IG ORDER ##############
            scan_results = pd.read_csv('../scan_results/uk_' + strategy + r'_ig_result.csv')
            print(strategy.upper() + " IG Scan Results:")
            print(scan_results.to_string(index=False))
            qty = orb.get_ig_sb_qunatity(scan_results, 1000)
            print(qty)
            orb.ig_place_orders(strategy, scan_results.head(5), qty)
