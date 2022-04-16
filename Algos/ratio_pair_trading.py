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
from binance.exceptions import BinanceAPIException


""" HELPER FUNCTIONS """


def prepare_for_trading():
    # Set margin type and leverage
    for ticker in tickers_to_trade:
        try:
            client.futures_change_margin_type(symbol=ticker, marginType='CROSSED')
        except BinanceAPIException:
            # TODO: Logging
            pass
        try:
            client.futures_change_leverage(symbol=ticker, leverage=leverage)
        except BinanceAPIException:
            # TODO: Logging
            pass

    # Check we are in single asset mode, if not make it so
    # client.futures_get_multi_assets_mode() does not work

    # Check and set position mode
    res = client.futures_get_position_mode()
    if res['dualSidePosition'] is not False:
        # TODO: Logging
        client.futures_change_position_mode(dualSidePosition=False)


def check_system_status():
    # Technically does not hit the testnet if using testnet client, but both baseurls are on the same domain...
    response = client.get_system_status()
    return response['status']  # if 0: normal. if 1: system maintenance


def get_open_pair_positions():
    # using pair_map, get which spreads we are currently invested in

    # First, get position information of all tickers_to_trade
    positions = []
    for ticker in tickers_to_trade:
        res = client.futures_position_information(symbol=ticker)
        if res[0]['positionAmt'] != 0:
            positions.append(res[0]['symbol'])

    # If we have no positions, we can exit out of the function here
    if len(positions) == 0:
        return []

    # Check which open positions are in the pair_map to deduce which spreads we are currently trading
    pairs = []
    for k, v in pair_map.items():
        if k in positions and v in positions:
            pairs.append(f'{k}/{v}')

    return pairs


def compute_return_on_spread(pair):
    # Compute the %-return from trading the spread, for a given pair - use get_open_pair_positions()
    # For now, we are going to use the mark price for unrealized return

    top_ticker = pair.split('/')[0]
    bot_ticker = pair.split('/')[1]

    # Get position information
    top_position = client.futures_position_information(symbol=top_ticker)
    bot_position = client.futures_position_information(symbol=bot_ticker)

    # Grab and compute relevant current notional values
    top_current_notional_value = float(top_position[0]['notional'])
    bot_current_notional_value = float(bot_position[0]['notional'])
    abs_total_current_notional = abs(top_current_notional_value) + abs(bot_current_notional_value)

    # Compute initial notional values
    top_initial_notional_value = float(top_position[0]['positionAmt']) * float(top_position[0]['entryPrice'])
    bot_initial_notional_value = float(bot_position[0]['positionAmt']) * float(bot_position[0]['entryPrice'])

    # Compute unrealized pnl in dollar terms
    top_unrealized_pnl = top_current_notional_value - top_initial_notional_value
    bot_unrealized_pnl = bot_current_notional_value - bot_initial_notional_value

    # Compute weights for each currency in the pair. Should ideally be around 50/50
    top_weight = abs(top_current_notional_value) / abs_total_current_notional
    bot_weight = abs(bot_current_notional_value) / abs_total_current_notional

    # Compute %-returns on each currency
    top_unrealized_ret = (top_unrealized_pnl) / top_initial_notional_value * leverage
    bot_unrealized_ret = (bot_unrealized_pnl) / bot_initial_notional_value * leverage

    # Computed the weighted average / return on this spread position
    spread_return = (top_unrealized_ret * top_weight) + (bot_unrealized_ret * bot_weight)

    return spread_return


def cancel_open_orders():
    for i in tickers_to_trade:
        client.futures_cancel_all_open_orders(symbol=i)

    return


def close_all_positions():

    # TODO: Better to use client.futures_place_batch_order() ?

    res = client.futures_account()

    for ticker in tickers_to_trade:
        for position in res['positions']:
            if position['symbol'] == ticker and float(position['positionAmt']) != 0:
                position_amount = float(position['positionAmt'])

                client.futures_create_order(symbol=ticker,
                                            side=SIDE_SELL if position_amount > 0 else SIDE_BUY,
                                            type=ORDER_TYPE_MARKET,
                                            quantity=abs(position_amount))
    return


def compute_quantities_to_trade(pair):
    top_ticker = pair.split('/')[0]
    bot_ticker = pair.split('/')[1]

    res = client.futures_account()
    available_capital = float(res['availableBalance'])
    investable_capital = available_capital*fraction_of_avbl_capital_to_trade
    investable_capital_pr_leg = investable_capital/2

    quantity_map = dict.fromkeys([top_ticker, bot_ticker], None)

    for ticker in [top_ticker, bot_ticker]:
        res = client.futures_symbol_ticker(symbol=ticker)
        last_price = float(res['price'])
        quantity = investable_capital_pr_leg/last_price
        quantity_map[ticker] = quantity

    return quantity_map


def open_single_pair_position(quantity_map, long):

    # long: bool. do we long or short the spread

    # Does not work :(
    # batch = [
    #    {
    #        'symbol': 'BTCUSDT',
    #        'side': SIDE_SELL,
    #        'type': ORDER_TYPE_MARKET,
    #        'quantity': 0.3
    #    },
    #    {
    #        'symbol': 'ETHUSDT',
    #        'side': SIDE_BUY,
    #        'type': ORDER_TYPE_MARKET,
    #        'quantity': 0.652
    #    }
    # ]
    # client.futures_place_batch_order(batchOrders=batch)

    for k, v in quantity_map.items():
        if long:
            client.futures_create_order(symbol=k,
                                        side=SIDE_BUY if k == list(quantity_map.keys())[0] else SIDE_SELL,
                                        type=ORDER_TYPE_MARKET,
                                        quantity=round(v, 3))
        else:
            client.futures_create_order(symbol=k,
                                        side=SIDE_SELL if k == list(quantity_map.keys())[0] else SIDE_BUY,
                                        type=ORDER_TYPE_MARKET,
                                        quantity=round(v, 3))


def close_single_pair_position(pair):
    top_ticker = pair.split('/')[0]
    bot_ticker = pair.split('/')[1]

    tickers = [top_ticker, bot_ticker]
    for ticker in tickers:
        position = client.futures_position_information(symbol=ticker)
        position_amount = float(position[0]['positionAmt'])

        if position_amount != 0:
            client.futures_create_order(symbol=ticker,
                                        side=SIDE_SELL if position_amount > 0 else SIDE_BUY,
                                        type=ORDER_TYPE_MARKET,
                                        quantity=abs(position_amount))



""" MAIN FUNCTIONS """


def start_multiplex_socket(testnet):
    global twm_market

    try:
        twm_market = ThreadedWebsocketManager(api_key=api_key, api_secret=api_secret, testnet=testnet)
        twm_market.start()
        twm_market.start_futures_multiplex_socket(callback=process_market_messages, streams=streams)
        return True

    except:
        twm_market.stop()
        return False


def start_account_socket(testnet):
    global twm_account

    # TODO: this method does raises asyncio error on testnet
    # TODO: Try with client.futures_stream_get_listen_key() and keepalive

    try:
        twm_account = ThreadedWebsocketManager(api_key=api_key, api_secret=api_secret, testnet=testnet)
        twm_account.start()
        twm_account.start_futures_user_socket(callback=process_account_messages)

        return True

    except:
        twm_account.stop()
        return False


def process_market_messages(msg):
    global socket_status, df_market_messages, df_most_recent_ratios

    # If we disconnected and could not reconnect
    if msg['data']['e'] == 'error':
        # TODO: Logger
        socket_status = False
        twm_market.stop()
        sleep(2)
        socket_status = start_multiplex_socket(testnet=testnet)

    else:
        # Part of this code block is reusable
        market_data_cols = ['Event time',
                            'Ticker',
                            'Kline open time',
                            'Kline close time',
                            'Open price',
                            'Close price',
                            'High price',
                            'Low price',
                            'Volume']
        # Websocket message sub-fields under the "k" key. Must match with market_data_cols
        k_fields_to_grab = ['t', 'T', 'o', 'c', 'h', 'l', 'v']

        # Create row to insert
        row = [msg['data']['E'], msg['data']['ps']]
        row.extend([msg['data']['k'][i] for i in k_fields_to_grab])

        sub_frame = pd.DataFrame(data=[row], columns=market_data_cols)

        # Change dtype to float and int
        sub_frame[['Open price', 'Close price', 'High price', 'Low price', 'Volume']] = sub_frame[
            ['Open price', 'Close price', 'High price', 'Low price', 'Volume']].astype(float)

        # Convert timestamps to aware datestime objects. tz = 'Europe/Oslo'
        time_cols = [i for i in sub_frame.columns if ('time' in i)]

        for col in time_cols:
            sub_frame[col] = pd.to_datetime(sub_frame[col].astype(str).str[:-3], unit='s').dt.tz_localize(
                'UTC').dt.tz_convert('Europe/Oslo')

        df_market_messages = pd.concat([df_market_messages, sub_frame], ignore_index=True)

        # Prevent df_master from growing past 100 rows, only grab the last 100
        df_market_messages = df_market_messages.iloc[-100:]

        # The rest of this code block will be less reusable
        # Drop cols we do not need and compute ratio of prices by matching nearest close prices for each pair
        df_market_messages = df_market_messages[['Event time', 'Ticker', 'Close price']].copy()
        df_market_messages.sort_values(by=['Event time', 'Ticker'], ascending=False, inplace=True)

        # We have "duplicates" in some rows because we discard the millisecond part of the timestamp
        df_market_messages = df_market_messages[
            ~df_market_messages.duplicated(subset=['Event time', 'Ticker'], keep='first')]
        df_market_messages.reset_index(drop=True, inplace=True)

        # When starting the socket, we will have the case where we don't have a datapoint for every ticker
        if set(tickers_to_trade) != set(df_market_messages['Ticker']):
            return

        # Iterate over pair_map and compute ratios
        for (k, v) in pair_map.items():
            df_most_recent_ratios[f'{k}/{v}'].iloc[0] = (
                        df_market_messages[df_market_messages['Ticker'] == k].iloc[0]['Close price'] /
                        df_market_messages[df_market_messages['Ticker'] == v].iloc[0]['Close price'])

        # Normalize the ratios using param_map
        for col in df_most_recent_ratios.columns:
            df_most_recent_ratios[col] = df_most_recent_ratios[col].apply(
                lambda x: (x - param_map[f'{col}_mu']) / param_map[f'{col}_sigma'])


def process_account_messages(msg):
    print(msg)


def run_trade_logic():
    try:
        # TODO: Potential issues: we may be invested in a spread for too long, without it giving any losses or profits
        # TODO: Position sizing is static, lel

        # Get open pairs
        pairs = get_open_pair_positions()

        # If we have open pairs, check if we want to close out the position
        if len(pairs) != 0:
            for pair in pairs:
                spread_return = compute_return_on_spread(pair)
                if spread_return < take_profit_level and spread_return > stop_loss_level:
                    pass
                elif spread_return > take_profit_level or spread_return < stop_loss_level:
                    close_single_pair_position(pair)
                    # TODO: Logging

        # We may have sold some pairs, so we need to refresh the list
        pairs = get_open_pair_positions()

        for k, v in pair_map.items():
            pair = f'{k}/{v}'
            if pair in pairs:
                # If we already have a position in this pair, and it was not closed out earlier, do nothing
                continue
            else:
                signal = df_most_recent_ratios[pair].iloc[0]

                # Long the spread
                if signal >= signal_buy:
                    quantity_map = compute_quantities_to_trade(pair)
                    open_single_pair_position(quantity_map=quantity_map, long=True)

                # Short the spread
                elif signal <= signal_sell:
                    quantity_map = compute_quantities_to_trade(pair)
                    open_single_pair_position(quantity_map=quantity_map, long=False)

                # Do nothing if signal is bound between the buy and sell signal threshold
                elif signal_buy > signal > signal_sell:
                    pass

        return True

    except:
        # TODO: logging
        return False


# import pytz
# timezone = pytz.timezone('Europe/Oslo')
# datetime.datetime.fromtimestamp(1650019591, tz=timezone)


def main():
    set_envrionment_vars(testnet=True)

    # Algo Settings
    global testnet, api_key, api_secret, streams, tickers_to_trade, pair_map, leverage
    global take_profit_level, stop_loss_level, signal_buy, signal_sell, fraction_of_avbl_capital_to_trade

    testnet = True
    api_key = os.environ.get('binance_api')
    api_secret = os.environ.get('binance_secret')
    streams = ['btcusdt_perpetual@continuousKline_1h', 'ethusdt_perpetual@continuousKline_1h']
    tickers_to_trade = ['BTCUSDT', 'ETHUSDT']
    pair_map = {
        'BTCUSDT': 'ETHUSDT'
    }
    leverage = 10
    recalibrate_frequency = 24  # In hours (how often to re-fit strategy)
    take_profit_level = 0.02
    stop_loss_level = -0.02
    signal_buy = 2
    signal_sell = -2
    fraction_of_avbl_capital_to_trade = 0.9 # of the total capital available to trade, how much of it will we use to compute quantities (of crypto)to trade

    # Data and other objects
    # TODO: twm_market need not be declared global in main() ?
    global param_map, twm_market, client, logger, df_market_messages, df_most_recent_ratios

    # logger = initiate_logger(os.path.basename(__file__))
    df_market_messages = pd.DataFrame()
    df_most_recent_ratios = pd.DataFrame(columns=[f'{k}/{v}' for k, v in pair_map.items()],
                                         index=[0])

    # Algo flags
    global run_algo, socket_status
    run_algo = True

    run_timestamp = datetime.datetime.now()
    next_recalibration_timestamp = run_timestamp + datetime.timedelta(hours=recalibrate_frequency)

    while run_algo is True:

        # First check that Binance is live
        status = check_system_status()
        while status == 1:
            sleep(600)
            status = check_system_status()
            # TODO: Logging

        try:
            client = Client(api_key, api_secret, testnet=testnet)
            strat = normalizedRatio(n=72, interval='1h', ticker_list=tickers_to_trade, pair_map=pair_map, client=client)
            param_map = strat.fit()
            socket_status = start_multiplex_socket(testnet=testnet)
            prepare_for_trading()
            counter = 0  # TODO: Delete the counter crap for production
            while socket_status is True and counter < 10:

                if datetime.datetime.now() >= next_recalibration_timestamp:
                    # Break out of inner loop to re-fit strategy
                    twm_market.stop()
                    break

                try:
                    # All strategy-driven trading happens here
                    socket_status = run_trade_logic()  # TODO: Should probably not return socket_status..?
                    sleep(5)
                    counter += 1

                except Exception as e:
                    # logger.critical(e, exc_info=True)
                    # logger.handlers.clear()
                    twm_market.stop()
                    run_algo = False
                    cancel_open_orders()
                    close_all_positions()
                    break

        except Exception as e:
            twm_market.stop()
            run_algo = False
            cancel_open_orders()
            close_all_positions()
            # logger.critical(e, exc_info=True)
            # logger.handlers.clear()

        twm_market.stop()
        print('ran all the way to the end')
        run_algo = False
        # TODO: Before we quit, we might want to exit all open positions

# if __name__ == '__main__':
#    main()
