import os
from Strategies.pairs_trading import normalizedRatio
from Modules.configure_api import set_envrionment_vars
from binance.client import Client
from binance import ThreadedWebsocketManager
from time import sleep

def normalizer(ratio):
    """
    Take a ratio of prices and make it standard normal, using param map
    :return: standardized ratio
    """
    return


def process_messages(msg):

     #If we disconnected and could not reconnect
    if msg['data']['e'] == 'error':
        twm.stop()
        sleep(2)
        twm.start()

    else:
        print(msg)


def main():

    global param_map, streamed_data, twm, streams
    set_envrionment_vars()
    api_key = os.environ.get('binance_api')
    api_secret = os.environ.get('binance_secret')
    tickers_to_trade = ['BTCUSDT', 'ETHUSDT']

    pair_map = {
        'BTCUSDT': 'ETHUSDT'
    }

    streamed_data = {f'{k}/{v}' for k, v in pair_map.items()}
    streams = ['btcusdt@kline_1h', 'ethusdt@kline_1h']


    try:
        client = Client(api_key, api_secret)
        strat = normalizedRatio(n=72, interval='1h', ticker_list=tickers_to_trade, pair_map=pair_map, client=client)
        param_map = strat.fit()  # TODO: Instead of returning a dict, maybe return a normalizer function, or define another method in the class to normalize
        twm = ThreadedWebsocketManager(api_key=api_key, api_secret=api_secret)
        twm.start()
        twm.start_multiplex_socket(callback=process_messages, streams=streams)

        # This runs after
        for i in range(10):
            print(i)

    except:
        twm.stop()
        print('total fatal error in except in main(), bro')

if __name__ == '__main__':
    main()

