"""
Collection of functions to return time-series of prices
"""
import datetime
import pandas as pd
from binance.enums import *


# TODO: Use timezones (either pytz or dateparser) to make the date part of the code more elegant
def historical_prices(symbols, interval, start, end, client, type):
    # YYYY-MM-DD (string) format on start and end or timestamps
    # type (string) spot or futures
    # Reformat dates to timestamps if they are YYYY-MM-DD strings

    if isinstance(start, str):
        start = datetime.datetime.strptime(start, '%Y-%m-%d').timestamp()

    if isinstance(end, str):
        end = datetime.datetime.strptime(end, '%Y-%m-%d').timestamp()

    prices_final = pd.DataFrame()
    for symbol in symbols:
        candles = client.get_historical_klines(symbol,
                                               interval,
                                               str(start),
                                               str(end),
                                               klines_type=HistoricalKlinesType.SPOT if type.lower() == 'spot' else HistoricalKlinesType.FUTURES)

        prices = pd.DataFrame(columns=['Open Time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Close Time'],
                              index=[i for i in range(len(candles))])

        for idx, candle in enumerate(candles):
            prices.loc[idx] = candle[0:len(prices.columns)]

        prices['Open Time'] = pd.to_datetime(prices['Open Time'], unit='ms')
        prices['Close Time'] = pd.to_datetime(prices['Close Time'], unit='ms')
        prices['Ticker'] = symbol

        prices_final = pd.concat([prices_final, prices], ignore_index=True)

    prices_final.sort_values(by=['Ticker', 'Open Time', 'Close Time'], ascending=False, inplace=True)
    prices_final.reset_index(drop=True, inplace=True)
    return prices_final
