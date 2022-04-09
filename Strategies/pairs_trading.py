import datetime
import pandas as pd
import numpy as np
from Modules.get_historical_data import historical_prices

class normalizedRatio:
    def __init__(self, n, interval, ticker_list, pair_map, client):
        """
        :param n: (int) amount of periods to use for normalization
        :param ticker_list: (lst) list of symbols
        :param pair_map: (dict) mapping of which pairs to trade
        """
        self.n = n
        self.ticker_list = ticker_list
        self.interval = interval
        self.map = pair_map
        self.params = dict()
        self.client = client


    def fit(self):
        start = datetime.datetime(2003,1,1).timestamp()  # Grabs all data available, essentially
        end = datetime.datetime.now().timestamp()
        df = historical_prices(self.ticker_list, self.interval, start, end, self.client)

        # Only keep the the n last data points
        df_trimmed = pd.DataFrame()
        for ticker in  df.Ticker.unique():
            sub_frame = df[df['Ticker'] == ticker].copy()
            sub_frame = sub_frame.iloc[0:self.n]
            df_trimmed = pd.concat([df_trimmed, sub_frame], ignore_index=True)

        df = df_trimmed.copy()

        # Format DataFrame such that we have timeseries of equal length for each ticker
        lengths = []
        for ticker in df.Ticker.unique():
            length = len(df[df['Ticker'] == ticker])
            lengths.append(length)

        # Find shortest length and trim all series to this length in case they not all have length n
        min_length = np.min(lengths)
        if min_length != self.n:
            df_trimmed = pd.DataFrame()
            for ticker in df.Ticker.unique():
                sub_frame = df[df['Ticker'] == ticker].copy()
                sub_frame = sub_frame.iloc[0:min_length]
                df_trimmed = pd.concat([df_trimmed, sub_frame], ignore_index=True)

            df_trimmed.reset_index(drop=True, inplace=True)

        # Create new DataFrame with price ratios
        df_ratio = pd.DataFrame(columns=list(df.columns), index=[i for i in range(len(self.map.keys())*min_length)])
        df_ratio['Open Time'] = df['Open Time'].unique()
        df_ratio['Close Time'] = df['Close Time'].unique()
        for col in [i for i in df.columns if not ('Time' in i)]:
            for k,v in self.map.items():
                if col == 'Ticker':
                    df_ratio[col] = f'{k}/{v}'
                else:
                    df_ratio[col] = df_trimmed[df_trimmed['Ticker'] == k][col] / df_trimmed[df_trimmed['Ticker'] == v][col]

        # Compute mu and sigma of all ratios
        # For now, we are going to strictly use the Open price for this.
        # Later revisions may include some kind of weighting in computing the statistical moments
        moments = ['mu', 'sigma']
        pairs = df_ratio['Ticker'].unique()

        for pair in pairs:
            for moment in moments:
                if moment == 'mu':
                    self.params[f'{pair}_{moment}'] = df_ratio[df_ratio['Ticker'] == pair]['Open'].astype(float).mean()

                else:
                    self.params[f'{pair}_{moment}'] = df_ratio[df_ratio['Ticker'] == pair]['Open'].astype(float).std()

        return self.params

