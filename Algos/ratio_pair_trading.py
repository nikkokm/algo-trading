import os
from Strategies.pairs_trading import normalizedRatio
from Modules.configure_api import set_envrionment_vars
from binance.client import Client
from binance import ThreadedWebsocketManager
from time import sleep


""" MAIN FUNCTIONS """


def start_multiplex_socket():
    try:
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
        # Main trade logic here
        return True

    except:
        # TODO: logging
        return False



def process_messages(msg):
    # If we disconnected and could not reconnect
    if msg['data']['e'] == 'error':
        twm.stop()
        sleep(2)
        start_multiplex_socket()

    else:
        print(msg)


def main():
    global param_map, streamed_data, twm, streams, api_key, api_secret
    global socket_status, run_algo

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
        try:
            client = Client(api_key, api_secret)
            strat = normalizedRatio(n=72, interval='1h', ticker_list=tickers_to_trade, pair_map=pair_map, client=client)
            param_map = strat.fit()
            twm = ThreadedWebsocketManager(api_key=api_key, api_secret=api_secret)
            socket_status = start_multiplex_socket()

            while socket_status is True:  # and twm.is_alive() is True: ???
                socket_status = run_trade_logic()

        except:
            twm.stop()
            print('total fatal error in except in main(), bro')
            run_algo = False
            # TODO: logging and notifying via emial
            # TODO: Here we can set conditions (or have different except statements) to set run_algo to False and quit
            # TODO: Before we quit, we might want to exit all open positions

        print('if this runs, socket_status is False, i.e. the socket died / closed')


if __name__ == '__main__':
    main()
