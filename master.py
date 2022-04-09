import os
from Strategies.pairs_trading import normalizedRatio
from Modules.configure_api import set_envrionment_vars
from binance.client import Client


if __name__ == '__main__':

    set_envrionment_vars()
    api_key = os.environ.get('binance_api')
    api_secret = os.environ.get('binance_secret')

    client = Client(api_key, api_secret)
    tickers_to_trade = ['BTCUSDT', 'ETHUSDT']
    pair_map = {
        'BTCUSDT': 'ETHUSDT'
    }

    strat = normalizedRatio(n=72, interval='1h', ticker_list=tickers_to_trade, pair_map=pair_map, client=client)
    param_map = strat.fit()
