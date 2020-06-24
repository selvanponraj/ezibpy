import alpaca_trade_api as tradeapi
import pandas as pd
import csv
from datetime import timedelta
import datetime
from pytz import timezone
import ezibpy
import time


API_KEY = "PK26EO0HRJ9C4I5F1ZW6"
API_SECRET = "0Ai1yhlabukF7KtWsrY8XeNBC7s2CGZZKuMNDkjf"
APCA_API_BASE_URL = "https://paper-api.alpaca.markets"

# API datetimes will match this format. (-04:00 represents the market's TZ.)
api_time_format = '%Y-%m-%dT%H:%M:%S-04:00'

class Orb:
    def __init__(self):
        self.alpaca = tradeapi.REST(API_KEY, API_SECRET, APCA_API_BASE_URL, 'v2')

    def get_qunatity(self,orbs_df, risk):
        shares = {}
        for _, row in orbs_df.iterrows():
            shares[row['symbol']] = int(
                risk/((row['high']+row['low'])/2)
            )
        return shares

    # Returns a string version of a timestamp compatible with the Alpaca API.
    def api_format(self,dt):
        return dt.strftime(api_time_format)

    def alpaca_scanners(self,strategy,source,timeframe='15Min'):
        data_dict = {}
        if strategy == 'orb':
            window_size = 5
            with open('../resources/' + source + '.csv') as file:
                data_dict = dict(filter(None, csv.reader(file)))
                selected_stocks = data_dict.keys()
        else:
            window_size = 5
            with open('../resources/'+source+'.csv') as file:
                selected_stocks = [line.split(',')[0].strip() for line in file]

        orbs = pd.DataFrame(columns=['symbol', 'high', 'low', 'edge'])

        assets = self.alpaca.list_assets()
        all_symbols = [asset.symbol for asset in assets]

        scan_symbols = set(all_symbols).intersection(set(selected_stocks))
        # scan_symbols = ['PH']

        if scan_symbols:
            barset = self.alpaca.get_barset(
                symbols=scan_symbols,
                timeframe=timeframe,
                limit=window_size,
                start=algo_start_time,
                end=algo_end_time
            )


        for symbol in [symbol for symbol in (scan_symbols or [])]:
            bars = barset[symbol]
            # print(symbol)
            # print(bars.df)
            if strategy == 'orb' and len(bars) > 4:
                open_candle = bars[0]
                second_candle = bars[1]
                third_candle = bars[2]
                fourth_candle = bars[3]

                # print(symbol)
                # print(bars.df)
                if ((second_candle.h < open_candle.h and second_candle.l > open_candle.l) and
                        (third_candle.h < open_candle.h and third_candle.l > open_candle.l) and
                        (fourth_candle.h < open_candle.h and fourth_candle.l > open_candle.l)):

                    print(symbol)
                    print(bars.df)
                    orbs = orbs.append({
                        'symbol': symbol,
                        'high': round(open_candle.h,2),
                        'low': round(open_candle.l,2),
                        'edge':float(data_dict[symbol])
                    }, ignore_index=True)
            elif strategy == '10am-buy' and len(bars) > 3:
                open_candle = bars[0]
                second_candle = bars[1]
                third_candle = bars[2]

                if ((second_candle.h < open_candle.h and second_candle.l > open_candle.l) and
                        (third_candle.h > open_candle.h) and (third_candle.c > open_candle.c)):
                    print(symbol)
                    print(bars.df)
                    orbs = orbs.append({
                        'symbol': symbol,
                        'high': round(open_candle.h,2),
                        'low': round(open_candle.l,2),
                        'edge': third_candle.v
                    }, ignore_index=True)
            elif strategy == '10am-sell' and len(bars) > 3:
                open_candle = bars[0]
                second_candle = bars[1]
                third_candle = bars[2]

                if ((second_candle.h < open_candle.h and second_candle.l > open_candle.l) and
                        (third_candle.l < open_candle.l) and third_candle.c < open_candle.c):
                    print(symbol)
                    print(bars.df)
                    orbs = orbs.append({
                        'symbol': symbol,
                        'high': round(open_candle.h,2),
                        'low': round(open_candle.l,2),
                        'edge': third_candle.v
                    }, ignore_index=True)

        orbs = orbs.sort_values(['edge'], ascending=[False])
        if not orbs.empty:
            open('../scan_results/us_'+strategy + r'_result.csv', 'w').close()
            orbs.to_csv('../scan_results/us_'+strategy + r'_result.csv', header=True, index=None, sep=',')
        return orbs;

    # Wait for market to open.

    def awaitMarketOpen(self):
        isOpen = self.alpaca.get_clock().is_open
        while (not isOpen):
            clock = self.alpaca.get_clock()
            openingTime = clock.next_open.replace(tzinfo=datetime.timezone.utc).timestamp()
            currTime = clock.timestamp.replace(tzinfo=datetime.timezone.utc).timestamp()
            timeToOpen = int((openingTime - currTime) / 60)
            print(str(timeToOpen) + " minutes til market open.")
            time.sleep(60)
            isOpen = self.alpaca.get_clock().is_open

    def place_orders(self,strategy,scan_results,quantity):
        for _, row in scan_results.iterrows():
            symbol = row['symbol']
            qty = quantity[symbol]
            cfd_contract = ibConn.createCFDContract(symbol)
            if strategy == 'orb':
                user_input = input(symbol + '- Buy - Would you like to place IB order (Yes/No)? ').upper()
                if user_input == 'YES':
                    # create an stop order - buy
                    buy_order = ibConn.createStopOrder(quantity=qty,price=row['high'], stop=row['high'], stop_limit=True)
                    # submit an order (returns order id)
                    buy_orderId = ibConn.placeOrder(cfd_contract, buy_order)
                user_input = input(symbol + '- Sell - Would you like to place IB order (Yes/No)? ').upper()
                if user_input == 'YES':
                    # create an stop order - sell
                    sell_order = ibConn.createStopOrder(quantity=-qty, price=row['low'], stop=row['low'], stop_limit=True)
                    # submit an order (returns order id)
                    sell_orderId = ibConn.placeOrder(cfd_contract, sell_order)
            elif strategy == '10am-buy':
                user_input = input(symbol + '- Buy - Would you like to place IB order (Yes/No)? ').upper()
                if user_input == 'YES':
                    order = ibConn.createBracketOrder(cfd_contract, quantity=qty, entry=row['high'], stop=row['low'])
            elif strategy == '10am-sell':
                user_input = input(symbol + '- Sell - Would you like to place IB order (Yes/No)? ').upper()
                if user_input == 'YES':
                    order = ibConn.createBracketOrder(cfd_contract, quantity=-qty, entry=row['low'], stop=row['high'])
            # let order fill
            time.sleep(3)

if __name__ == '__main__':
    orb = Orb()

    algo_time = timezone('UTC').localize(datetime.datetime.today() - timedelta(days=0))
    algo_start_time = algo_time.replace(hour=9).replace(minute=30).replace(second=00).strftime(api_time_format)
    algo_end_time = algo_time.replace(hour=10).replace(minute=30).replace(second=00).strftime(api_time_format)

    # initialize ezIBpy
    ibConn = ezibpy.ezIBpy()
    ibConn.connect(clientId=101, host="localhost", port=7496)

    strategy = 'orb'
    source = "orb_us_stocks"

    user_input = input('Would you like run Scanner (Yes/No)? ').upper()
    if user_input == 'YES':
        scan_results = orb.alpaca_scanners(strategy, source)
        print(strategy + " Scan Results:")
        print(scan_results.to_string(index=False))

    user_input = input('Would you like to place orders (Yes/No)? ').upper()
    if user_input == 'YES':
        print("Placing Orders ....")
        scan_results = pd.read_csv('../scan_results/us_' + strategy + r'_result.csv')
        print(scan_results)
        qty = orb.get_qunatity(scan_results, 5000)
        print(qty)
        orb.place_orders(strategy,scan_results.head(5), qty)



