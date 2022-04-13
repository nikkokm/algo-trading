import os
import datetime
from Strategies.pairs_trading import normalizedRatio
from Modules.configure_api import set_envrionment_vars
from Modules.algo_logger import initiate_logger
from binance.client import Client
from binance import ThreadedWebsocketManager
from time import sleep


""" HELPER FUNCTIONS """


def check_system_status():
    response = client.get_system_status()
    return response['status'] # if 0: normal. if 1: system maintenance


def close_all_positions():
    pass


""" MAIN FUNCTIONS """


def start_multiplex_socket():
    global twm
    try:
        twm = ThreadedWebsocketManager(api_key=api_key, api_secret=api_secret)
        twm.start()
        twm.start_multiplex_socket(callback=process_messages, streams=streams)
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
        return True

    except:
        # TODO: logging
        return False


def process_messages(msg):
    # If we disconnected and could not reconnect
    if msg['data']['e'] == 'error':
        # TODO: Logger
        twm.stop()
        sleep(2)
        start_multiplex_socket()

    else:
        print(msg)


def main():
    global param_map, streamed_data, twm, streams, api_key, api_secret, client
    global socket_status, run_algo, logger

    recalibrate_frequency = 24  # In hours

    run_timestamp = datetime.datetime.now()
    next_recalibration_timestamp = run_timestamp + datetime.timedelta(hours=recalibrate_frequency)

    #logger = initiate_logger(os.path.basename(__file__))

    set_envrionment_vars()

    run_algo = True
    api_key = os.environ.get('binance_api')
    api_secret = os.environ.get('binance_secret')
    tickers_to_trade = ['BTCUSDT', 'ETHUSDT']

    pair_map = {
        'BTCUSDT': 'ETHUSDT'
    }

    streamed_data = {f'{k}/{v}' for k, v in pair_map.items()}
    streams = ['btcusdt@kline_1h', 'ethusdt@kline_1h']

    while run_algo is True:

        # First check that Binance is live
        status = check_system_status()

        if status == 1:
            sleep(600)
            # TODO: Logging


        try:
            client = Client(api_key, api_secret)

            #strat = normalizedRatio(n=72, interval='1h', ticker_list=tickers_to_trade, pair_map=pair_map, client=client)
            #param_map = strat.fit()

            socket_status = start_multiplex_socket()
            close_all_positions()
            counter = 0
            while socket_status is True and counter < 10:  # and twm.is_alive() is True: ??? or maybe "and keep_trading

                if datetime.datetime.now() >= next_recalibration_timestamp:
                    break

                try:
                    socket_status = run_trade_logic()  # TODO: Should probably not return socket_status..?
                    sleep(5)
                    counter += 1

                except Exception as e:
                    # logger.critical(e, exc_info=True)
                    # logger.handlers.clear()
                    run_algo = False
                    close_all_positions()
                    break

        except Exception as e:
            twm.stop()
            print('total fatal error in except in main(), bro')
            run_algo = False
            close_all_positions()
            # logger.critical(e, exc_info=True)
            # logger.handlers.clear()


        twm.stop()
        print('ran all the way to the end')
        run_algo = False
        # TODO: Before we quit, we might want to exit all open positions



if __name__ == '__main__':
    main()
