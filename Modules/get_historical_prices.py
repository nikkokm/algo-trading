"""
Collection of functions to return time-series of prices
"""
import datetime
import pandas as pd


# TODO: Does this get future or spot price??
# TODO: What timezone are the timestamps in
def historical_prices(symbol, interval, start, end):

    # Reformat dates to timestamps
    start_timestamp = datetime.datetime.strptime(start, '%Y-%m-%d').timestamp()
    end_timestamp = datetime.datetime.strptime(end, '%Y-%m-%d').timestamp()

    candles = client.get_historical_klines(symbol, interval, str(start_timestamp), str(end_timestamp))

    prices = pd.DataFrame(columns=['Open Time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Close Time'],
                          index=[i for i in range(len(candles))])
    for idx, candle in enumerate(candles):
        prices.loc[idx] = candle[0:len(prices.columns)]

    prices['Open Time'] = pd.to_datetime(prices['Open Time'], unit='ms')
    prices['Close Time'] = pd.to_datetime(prices['Close Time'], unit='ms')
    prices['Ticker'] = symbol

    return prices






