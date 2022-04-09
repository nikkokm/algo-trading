import datetime
import pandas as pd
import numpy as np
from Modules.get_historical_data import historical_prices

class normalizedRatio:
    def __init__(self, n, interval, ticker_list, pair_map):
        """
        :param n: (int) amount of periods to use for normalization
        :param ticker_list: (lst) list of symbols
        :param pair_map: (dict) mapping of which pairs to trade
        """
        self.n = n
        self.ticker_list = ticker_list
        self.interval = interval
        self.map = pair_map

    def fit(self):
        start = datetime.datetime(2003,1,1).timestamp()  # Grabs all data available, essentially
        end = datetime.datetime.now().timestamp()
        df = historical_prices(self.ticker_list, self.interval, self.start, self.end)

        # Format DataFrame such that we have timeseries of equal length for each ticker
        lengths = []
        for ticker in df.Tickers.unique():
            length = len(df[df['Tickers'] == ticker])
            lengths.append(length)

        # Find shortest length and trim all series to this length
        min_length = np.min(lengths)
        df_trimmed = pd.DataFrame()
        for ticker in df.Tickers.unique():
            sub_frame = df[df['Tickers'] == ticker].copy()
            sub_frame = sub_frame.iloc[0:min_length]
            df_trimmed = pd.concat([df_trimmed, sub_frame], ignore_index=True)

        df_trimmed.reset_index(drop=True, inplace=True)

        # Create new dataframe with price ratios
        df_ratio = pd.DataFrame(columns=list(df.columns), index=[i for i in range(len(self.ticker_list)/2*min_length)])
        df_ratio['Open Time'] = df['Open Time'].unique().values
        df_ratio['Close Time'] = df['Close Time'].unique().values
        for col in [i for i in df.columns if not ('Time' in i)]:
            for k,v in self.map.items():
                if col == 'Ticker':
                    df_ratio[col] = f'{k}/{v}'
                else:
                    df_ratio[col] = df_trimmed[df_trimmed['Tickers'] == k][col] / df_trimmed[df_trimmed['Tickers'] == v][col]

        # Compute mu and sigma of all ratios


