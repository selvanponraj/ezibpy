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

    def get_qunatity(self,orbs_df, risk):
        shares = {}
        for _, row in orbs_df.iterrows():
            shares[row['symbol']] = int(
                (risk/((row['high']+row['low'])/2))*100
            )
        return shares

    def get_ig_sb_qunatity(self,orbs_df, risk):
        try:
            shares = {}
            for _, row in orbs_df.iterrows():
                avg_price = (row['high']+row['low'])/2
                margin_required = avg_price * 0.20
                qty = round(float(risk/margin_required),1)
                if qty < 0.5:
                    qty =0.5
                shares[row['epic']] = qty
            return shares
        except:
            logger.error("Unable to get Quantity, Verify Scan Result has both high and low price columns")
            exit(2)


    def ig_scanners(self,strategy,source,timeframe='15Min'):
        orbs = pd.DataFrame(columns=['symbol', 'epic', 'high', 'low', 'edge'])
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
            num_points = window_size
            #(start_date, end_date) = ('2020-06-15T6:00:00', '2020-06-15T7:00:00')
            # (start_date, end_date) = (algo_start_time, algo_end_time)
            num_points = 4
            try:
                response = self.ig_service.fetch_historical_prices_by_epic_and_date_range(epic, resolution, start_date, end_date)
            except:
                logger.error("Historical Data Not found- Try  after 8:30AM UK time")
                exit(2)
            bars = response['prices']['last']

            if strategy == 'orb' and len(bars) > 4:
                open_candle = bars.iloc[0]
                second_candle = bars.iloc[1]
                third_candle = bars.iloc[2]
                fourth_candle = bars.iloc[3]
                candle_total_volume = bars['Volume'].sum()

                if ((second_candle['High'] < open_candle['High'] and second_candle['Low'] > open_candle['Low']) and
                        (third_candle['High'] < open_candle['High'] and third_candle['Low'] > open_candle['Low']) and
                        (fourth_candle['High'] < open_candle['High'] and fourth_candle['Low'] > open_candle['Low'])):

                    print(symbol)
                    print(bars)

                    orbs = orbs.append({
                        'symbol': symbol,
                        'epic': epic.replace('CASH', 'DAILY'),
                        'high': round(open_candle['High'], 2),
                        'low': round(open_candle['Low'], 2),
                        'edge': float(data_dict[stock])
                    }, ignore_index=True)
            elif strategy == '10am-buy' and len(bars) > 3:
                open_candle = bars.iloc[0]
                second_candle = bars.iloc[1]
                third_candle = bars.iloc[2]
                candle_total_volume = bars['Volume'].sum()



                if ((second_candle['High'] < open_candle['High'] and second_candle['Low'] > open_candle['Low']) and
                        (third_candle['High'] > open_candle['High'] and (third_candle['Close'] > open_candle['Close']))):
                    print(symbol)
                    print(bars)
                    orbs = orbs.append({
                        'symbol': symbol,
                        'epic': epic.replace('CASH', 'DAILY'),
                        'high': round(open_candle['High'], 2),
                        'low': round(open_candle['Low'], 2),
                        'edge': candle_total_volume
                    }, ignore_index=True)
            elif strategy == '10am-sell' and len(bars) > 3:
                open_candle = bars.iloc[0]
                second_candle = bars.iloc[1]
                third_candle = bars.iloc[2]
                candle_total_volume = bars['Volume'].sum()

                # print((second_candle['High'] < open_candle['High'] and second_candle['Low']  > open_candle['Low'] ))
                # print((third_candle['Low']  < open_candle['Low']  and third_candle['Close'] < open_candle['Close']))

                if ((second_candle['High'] < open_candle['High'] and second_candle['Low']  > open_candle['Low'] ) and
                        (third_candle['Low']  < open_candle['Low']  and third_candle['Close'] < open_candle['Close'])):
                    print(symbol)
                    print(bars)
                    orbs = orbs.append({
                        'symbol': symbol,
                        'epic': epic.replace('CASH', 'DAILY'),
                        'high': round(open_candle['High'], 2),
                        'low': round(open_candle['Low'], 2),
                        'edge': candle_total_volume
                    }, ignore_index=True)
        orbs = orbs.sort_values(['edge'], ascending=[False])
        if not orbs.empty:
            open('../scan_results/us_' + strategy + r'_result.csv', 'w').close()
            orbs.to_csv('../scan_results/uk_' + strategy + r'_result.csv', header=True, index=None, sep=',')
        return orbs;

    def ib_place_orders(self,strategy,scan_results,quantity):
        for _, row in scan_results.iterrows():
            symbol = row['symbol']
            qty = quantity[symbol]
            risk = ((row['high'] - row['low']) * qty) + 6
            cfd_contract = ibConn.createCFDContract(symbol,'GBP')


            if strategy == 'orb':
                print(' Profit : 100' + '-Risk : ' + str(risk), + '-RR :' + str(100 / risk))
                user_input = input(str(symbol) + '- Buy - Would you like to place IB order (Yes/No)? ').upper()
                if user_input == 'YES':
                    # # create an stop order - buy
                    # buy_order = ibConn.createStopOrder(quantity=qty,price=row['high'], stop=row['high'], stop_limit=True)
                    # # submit an order (returns order id)
                    # buy_orderId = ibConn.placeOrder(cfd_contract, buy_order)
                    target = row['high'] + (100 / qty) * 100
                    buy_orderId = ibConn.createBracketOrder(cfd_contract, quantity=qty, entry=row['high'], target=target)
                    print(' Profit : 100' + '-Risk : ' + str(risk), + '-RR :' + str(100/risk))
                user_input = input(str(symbol) + '-Sell - Would you like to place IB order (Yes/No)? ').upper()
                if user_input == 'YES':
                    # # create an stop order - sell
                    # sell_order = ibConn.createStopOrder(quantity=-qty, price=row['low'], stop=row['low'], stop_limit=True)
                    # # submit an order (returns order id)
                    # sell_orderId = ibConn.placeOrder(cfd_contract, sell_order)
                    target = row['low'] - (100 / qty) * 100
                    sell_orderId = ibConn.createBracketOrder(cfd_contract, quantity=-qty, entry=row['low'], target=row['high'])

            elif strategy == '10am-buy':
                user_input = input(str(symbol) + '- Buy - Would you like to place IB order (Yes/No)? ').upper()
                if user_input == 'YES':
                    order = ibConn.createBracketOrder(cfd_contract, quantity=qty, entry=row['high'], stop=row['low'])
            elif strategy == '10am-sell':
                user_input = input(str(symbol) + '- Sell - Would you like to place IB order (Yes/No)? ').upper()
                if user_input == 'YES':
                    order = ibConn.createBracketOrder(cfd_contract, quantity=-qty, entry=row['low'], stop=row['high'])
            # let order fill
            time.sleep(3)

    def ig_place_orders(self, strategy, scan_results, quantity):
        for _, row in scan_results.iterrows():
            epic = row['epic']
            qty = quantity[epic]

            if strategy == 'orb':
                user_input = input(epic + ' - BUY - Would you like to place order (Yes/No)? ').upper()
                if user_input == 'YES':
                    response = self.ig_service.create_working_order('GBP', 'BUY', epic, 'DFB', False, row['high'], qty,
                                                                    'GOOD_TILL_CANCELLED', 'STOP', None, None, None, None
                                                                    )
                    print(response)
                    if response['dealStatus'] == 'REJECTED':
                        response = self.ig_service.create_working_order('GBP', 'BUY', epic, 'DFB', False, row['high'],
                                                                        qty,
                                                                        'GOOD_TILL_CANCELLED', 'LIMIT', None, None, None,
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
                                                                    'GOOD_TILL_CANCELLED', 'STOP', None, None, None, None)
                    print(response)
                    if response['dealStatus'] == 'REJECTED':
                        response = self.ig_service.create_working_order('GBP', 'SELL', epic, 'DFB', False, row['low'],
                                                                        qty, 'GOOD_TILL_CANCELLED', 'STOP', None, None, None,None)
                        print(response)
                        if response['dealStatus'] == 'REJECTED':
                            response = self.ig_service.create_open_position('GBP', 'SELL', epic, 'DFB', False, False,
                                                                            None, None, None, 'MARKET', None, qty, None,None, None, None)

                            print(response)
                else:
                    print("No Sell Order Placed for : " + epic)

            if strategy == '10am-buy':
                user_input = input(epic + ' - BUY - Would you like to place order (Yes/No)? ').upper()
                if user_input == 'YES':
                    response = self.ig_service.create_working_order('GBP', 'BUY', epic, 'DFB', False, row['high'], qty,
                                                                    'GOOD_TILL_CANCELLED', 'STOP', None, None, None,row['low'])
                    print(response)
                    if response['dealStatus'] == 'REJECTED':
                        response = self.ig_service.create_working_order('GBP', 'BUY', epic, 'DFB', False, row['high'],
                                                                        qty,'GOOD_TILL_CANCELLED', 'LIMIT', None, None, None,row['low'])

                        print(response)
                        if response['dealStatus'] == 'REJECTED':
                            response = self.ig_service.create_open_position('GBP', 'BUY', epic, 'DFB', False, False,
                                                                            None, None, None, 'MARKET', None, qty, None,None, None, None)
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
    algo_time = timezone('UTC').localize(datetime.datetime.today() - timedelta(days=0))
    algo_start_time = algo_time.replace(hour=8).replace(minute=00).replace(second=00).strftime(api_time_format)
    algo_end_time = algo_time.replace(hour=9).replace(minute=00).replace(second=00).strftime(api_time_format)
    algo_time = timezone('UTC').localize(datetime.datetime.today() - timedelta(days=0))
    position_end_time = algo_time.replace(hour=9).replace(minute=00).replace(second=00).strftime(api_time_format)

    # initialize ezIBpy
    ibConn = ezibpy.ezIBpy()
    ibConn.connect(clientId=100, host="localhost", port=7496)

    strategy = 'orb'
    source = "orb_uk_stocks_ig"

    user_input = input('Would you like run Scanner (Yes/No)? ').upper()
    if user_input == 'YES':
        scan_results = orb.ig_scanners(strategy, source)
        print(strategy + " Scan Results:")
        print(scan_results.to_string(index=False))

    user_input = input('Would you like to place orders (Yes/No)? ').upper()
    if user_input == 'YES':
        print("Placing Orders ....")
        scan_results = pd.read_csv('../scan_results/uk_' + strategy + r'_result.csv')
        print(scan_results)

        ########## IB ORDER ##############
        user_input = input('Would you like to place IB orders (Yes/No)? ').upper()
        if user_input == 'YES':
            qty = orb.get_qunatity(scan_results, 5000)
            print(qty)
            orb.ib_place_orders(strategy,scan_results.head(5), qty)

        user_input = input('Would you like to place IG orders (Yes/No)? ').upper()
        if user_input == 'YES':
            ########## IG ORDER ##############
            qty = orb.get_ig_sb_qunatity(scan_results, 1000)
            print(qty)
            orb.ig_place_orders(strategy,scan_results.head(5), qty)



