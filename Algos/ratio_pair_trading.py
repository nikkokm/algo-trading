import os
from Strategies.pairs_trading import normalizedRatio
from Modules.configure_api import set_envrionment_vars
from binance.client import Client
from binance import ThreadedWebsocketManager

def normalizer(ratio):
    """
    Take a ratio of prices and make it standard normal, using param map
    :return: standardized ratio
    """
    return


def trader(msg):
    """
    Main callback function for when web
    :return:
    """
    print(msg)


def main():
    global param_map, streamed_data
    set_envrionment_vars()
    api_key = os.environ.get('binance_api')
    api_secret = os.environ.get('binance_secret')
    tickers_to_trade = ['BTCUSDT', 'ETHUSDT']
    pair_map = {
        'BTCUSDT': 'ETHUSDT'
    }
    streamed_data = {f'{k}/{v}' for k, v in pair_map.items()}


    try:
        client = Client(api_key, api_secret)
        strat = normalizedRatio(n=72, interval='1h', ticker_list=tickers_to_trade, pair_map=pair_map, client=client)
        param_map = strat.fit()  # TODO: Instead of returning a dict, maybe return a normalizer function, or define another method in the class to normalize

    except:
        pass

if __name__ == '__main__':
    main()

