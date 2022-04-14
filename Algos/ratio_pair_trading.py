import os
import datetime
import pandas as pd
from time import sleep
from Strategies.pairs_trading import normalizedRatio
from Modules.configure_api import set_envrionment_vars
from Modules.algo_logger import initiate_logger
from binance.client import Client
from binance import ThreadedWebsocketManager
from binance.enums import *


""" HELPER FUNCTIONS """


def check_system_status():
    # Technically does not hit the testnet if using testnet client, but both baseurls are on the same domain...
    response = client.get_system_status()
    return response['status'] # if 0: normal. if 1: system maintenance


def cancel_open_orders():

    for i in tickers_to_trade:
        client.futures_cancel_all_open_orders(symbol=i)

    return


def close_all_positions():
    res = client.futures_account()

    for ticker in tickers_to_trade:
        for position in res['positions']:
            if position['symbol'] == ticker and float(position['positionAmt']) != 0:
                position_amount = float(position['positionAmt'])


                #if to_trade > 0:
                #    side = SIDE_BUY
                #else:
                #    side = SIDE_SELL

                client.futures_create_order(symbol=ticker,
                                           side=SIDE_SELL if position_amount > 0 else SIDE_BUY,
                                           type=ORDER_TYPE_MARKET,
                                           quantity=position_amount)
    return


""" MAIN FUNCTIONS """


def start_multiplex_socket():
    global twm

    try:
        twm = ThreadedWebsocketManager(api_key=api_key, api_secret=api_secret)
        twm.start()
        twm.start_futures_multiplex_socket(callback=process_market_messages, streams=streams)
        return True

    except:
        twm.stop()
        return False


def normalizer(ratio):
    """
    Take a ratio of prices and make it standard normal, using param map
    :return: standardized ratio
    """
    return


def run_trade_logic():
    try:
        print('doing some real trading in here yooo')
        # 1. Check current positions
        #     a. If Open positions, do risk management (whichever part of it cannot be baked into orders)
        #     b. if no open positions do nothing

        # 2. Check if BUY/SELL conditions met wrt. normalized ratio
        #     a. if NO, do nothing
        #     b. if YES and no current positions: calculate optimal portfolio and buy it
        #     c. if the signal is to LONG (SHORT) the spread but we are already LONG (SHORT), do nothing (for now)
        #     d. if we are LONG (SHORT) the spread, and the signal is to SHORT (LONG) the spread:
        #        Calculate optimal portfolio and necessary orders to move from current to optimal portfolio

        return True

    except:
        # TODO: logging
        return False


def process_market_messages(msg):
    global socket_status

    # If we disconnected and could not reconnect
    if msg['data']['e'] == 'error':
        # TODO: Logger
        socket_status = False
        twm.stop()
        sleep(2)
        socket_status = start_multiplex_socket()

    else:
        print(msg)


def main():
    global param_map, streamed_data, twm, streams, api_key, api_secret, client
    global socket_status, run_algo, logger, tickers_to_trade

    recalibrate_frequency = 24  # In hours

    run_timestamp = datetime.datetime.now()
    next_recalibration_timestamp = run_timestamp + datetime.timedelta(hours=recalibrate_frequency)

    #logger = initiate_logger(os.path.basename(__file__))

    set_envrionment_vars(testnet=True)

    run_algo = True
    api_key = os.environ.get('binance_api')
    api_secret = os.environ.get('binance_secret')
    tickers_to_trade = ['BTCUSDT', 'ETHUSDT']

    pair_map = {
        'BTCUSDT': 'ETHUSDT'
    }

    streams = ['btcusdt_perpetual@kcontinuousKine_1h', 'ethusdt_perpetual@continuousKline_1h']

    while run_algo is True:

        # First check that Binance is live
        status = check_system_status()

        while status == 1:
            sleep(600)
            status = check_system_status()
            # TODO: Logging


        try:
            client = Client(api_key, api_secret, testnet=True)

            #strat = normalizedRatio(n=72, interval='1h', ticker_list=tickers_to_trade, pair_map=pair_map, client=client)
            #param_map = strat.fit()

            socket_status = start_multiplex_socket()
            close_all_positions()
            counter = 0
            while socket_status is True and counter < 10:  # and twm.is_alive() is True: ??? or maybe "and keep_trading

                if datetime.datetime.now() >= next_recalibration_timestamp:
                    # Break out of inner loop to re-fit strategy
                    twm.stop()
                    break

                try:
                    # All strategy-driven trading happens here
                    socket_status = run_trade_logic()  # TODO: Should probably not return socket_status..?
                    sleep(5)
                    counter += 1

                except Exception as e:
                    # logger.critical(e, exc_info=True)
                    # logger.handlers.clear()
                    twm.stop()
                    run_algo = False
                    cancel_open_orders()
                    close_all_positions()
                    break

        except Exception as e:
            twm.stop()
            run_algo = False
            cancel_open_orders()
            close_all_positions()
            # logger.critical(e, exc_info=True)
            # logger.handlers.clear()


        twm.stop()
        print('ran all the way to the end')
        run_algo = False
        # TODO: Before we quit, we might want to exit all open positions


if __name__ == '__main__':
    main()
