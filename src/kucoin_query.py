from kucoin.client import Client
from kucoin.exceptions import KucoinAPIException
from datetime import datetime, timedelta
from time import sleep
import pandas as pd
import numpy as np
import json
from pathlib import Path


# Convert to/from kucoin timestamp (UNIX time in ms)
def to_timestamp(dt):
    return int(round(dt.timestamp() * 1000))
def from_timestamp(stamp):
    return datetime.fromtimestamp(stamp/1000)


with open(Path(__file__).parent / '../keys/kucoin-keys.json', 'r') as file:
    api_keys = json.load(file)
client = Client(api_keys['API_KEY'], api_keys['API_SECRET'], api_keys['API_PASSPHRASE'])
print("Connected to KuCoin API successfully!")


def try_call(fn, *args, **kwargs):
    while True:
        try:
            return fn(*args, **kwargs)
        except KucoinAPIException as e:
            if (e.code == '200002'):
                print("Reached request limit, taking a break...")
                sleep(3)
                continue
            else:
                print(e)
                break
    return None


def parse_fills(fills):
    df = pd.DataFrame(fills)
    df[['price', 'size', 'funds']] = df[['price', 'size', 'funds']].apply(pd.to_numeric, errors='coerce')
    df['createdAt'] = pd.to_datetime(df['createdAt'], unit='ms', utc=True)
    return df[::-1]


def parse_accounts(accounts):
    df = pd.DataFrame(accounts)
    df[['balance', 'available', 'holds']] = df[['balance', 'available', 'holds']].apply(pd.to_numeric, errors='coerce')
    return df


def find_start_date():
    start = datetime.utcnow() - timedelta(days=7)
    res = pd.Timestamp.now(tz='UTC')
    while True:
        stamp = to_timestamp(start)
        
        print(f"Checking week of {start.date()}")
        resp = try_call(client.get_fills, start=stamp, limit=10)
        if resp is None:
            break
                
        if resp['totalNum'] == 0:
            break
            
        fills_df = parse_fills(resp['items'])
        res = fills_df.iloc[0]['createdAt']
        
        start = start - timedelta(days=7)
        
    return res


def read_fills(start_ts=None, end_ts=None, query_size=500):
    if start_ts is None or pd.isna(start_ts):
        start_ts = find_start_date()
    if end_ts is None or pd.isna(end_ts):
        end_ts = pd.Timestamp.now(tz='UTC')
    
    all_fills_df = None
    
    while start_ts < end_ts:
        print(f"Reading week of {start_ts.date()}")    
        stamp = to_timestamp(start_ts)
        resp = try_call(client.get_fills, start=stamp, limit=query_size)
        if resp is None:
            break
        fills_df = parse_fills(resp['items'])
        
        num_pages = resp['totalPage']
        for page in range(2, num_pages):
            resp = try_call(client.get_fills, start=stamp, limit=query_size, page=page)
            if resp is None:
                break
            older_fills_df = parse_fills(resp['items'])
            fills_df = pd.concat([older_fills_df, fills_df], ignore_index=True)
            
        if all_fills_df is None:
            all_fills_df = fills_df
        else:
            all_fills_df = pd.concat([all_fills_df, fills_df], ignore_index=True)
            
        start_ts = start_ts + timedelta(days=7)
        
    return all_fills_df


stablecoins = {'USDT', 'USDC'}
def get_trades(start_time=None):
    fills_df = read_fills(start_ts=start_time)
    accounts_df = parse_accounts(try_call(client.get_accounts))
    
    # Parse and analyze fields
    fills_df[['targetCurrency', 'baseCurrency']] = fills_df['symbol'].str.split('-', expand=True)
    fills_df['coeff'] = (fills_df['side'] == 'buy') * 2 - 1
    fills_df['change'] = fills_df['size'] * fills_df['coeff']

    # Track change in each target currency
    fills_grouped_df = fills_df.groupby('targetCurrency')
    fills_df['runningTotal'] = fills_grouped_df['change'].cumsum()
    
    # Correlate running total to current account balance
    current_balance = accounts_df.groupby('currency')['balance'].sum()
    offset = current_balance - fills_grouped_df['runningTotal'].last()
    offset.name = 'offset'
    fills_df['runningTotal'] += fills_df.join(offset, on='targetCurrency', how='left')['offset']
    
    # Track trades through mediary currencies
    baseCurrencies = set(fills_df['baseCurrency'].unique())
    mediaryCurrencies = baseCurrencies - stablecoins
    
    fills_df['priceFiat'] = fills_df['price']
    for currency in mediaryCurrencies:
        # Buy trades use the last known mediary currency price (that we probably just bought)
        # Sell trades use the next known mediary currency price (that we are probably just about to sell)
        filt = (fills_df['targetCurrency'] == currency) & (fills_df['baseCurrency'] == 'USDT')
        prices_filt = fills_df['price'].where(filt)
        price_buy = prices_filt.fillna(method='ffill')
        price_sell = prices_filt.fillna(method='bfill')
        
        # Propagate prices through mediary currencies
        price_mediary = price_buy.where(fills_df['side'] == 'buy', price_sell)
        fills_df['priceFiat'].where(fills_df['baseCurrency'] != currency, fills_df['price'] * price_mediary, inplace=True)
        
    # Track change in fiat currency
    fills_df['changeFiat'] = fills_df['change'] * -fills_df['priceFiat']
    
    # Mark the close of a trade when we have less than $1.00 in that coin
    fills_df['close'] = np.abs(fills_df['runningTotal'] * fills_df['priceFiat']) < 1.00
    
    # The next trade opens a new trade and tradeGroup
    fills_df['open'] = fills_grouped_df['close'].shift(1).fillna(True)
    fills_df['tradeGroup'] = fills_grouped_df['open'].cumsum()
    
    # Now we can look at each individual trade within each target currency
    fills_trade_df = fills_df.groupby(['targetCurrency', 'tradeGroup'])
    fills_df['positionFiat'] = fills_trade_df['changeFiat'].cumsum()
    fills_df['positionCurrency'] = fills_trade_df['change'].cumsum()
    
    # Initial estimate of PNL - we will refine this later
    trade_pnl = fills_trade_df['changeFiat'].sum()
    
    # Track the time each investment in this trade lasts and use it to create a weighted average investment
    fills_df['timespan'] = -fills_trade_df['createdAt'].diff(periods=-1)
    fills_df['seconds'] = fills_df['timespan'].dt.total_seconds().fillna(0).clip(lower=0.001)
    fills_df['weightedInvestment'] = -fills_df['positionFiat'] * fills_df['seconds']
    avg_investment = fills_trade_df['weightedInvestment'].sum() / fills_trade_df['seconds'].sum()
    
    # Get the open and close time of each trade (no close time if the trade is still open)
    open_time = fills_trade_df['createdAt'].min()
    close_time = fills_trade_df['createdAt'].max().where(fills_trade_df['close'].any(), None)

    # Wrap up all our trade information into a new dataframe
    overview_df = pd.concat([avg_investment, trade_pnl, open_time, close_time], axis=1)
    overview_df.set_axis(['Investment', 'PNL', 'Open Time', 'Close Time'], axis=1, inplace=True)
    
    # Ignore trades of mediary currencies
    targetCurrencies = set(overview_df.index.get_level_values(0)) - baseCurrencies
    overview_nomed_df = overview_df.loc[targetCurrencies]
    
    # Find which currencies we still have open trades on
    openTrades = overview_nomed_df[pd.isna(overview_nomed_df['Close Time'])]
    openCurrencies = set(openTrades.index.get_level_values(0))
    
    # Find the current value of those open currencies
    curFiat = try_call(client.get_fiat_prices, symbol=openCurrencies)
    curFiat_df = pd.DataFrame.from_dict(curFiat, orient='index',dtype=float)
    curValue = curFiat_df.join(current_balance).product(axis='columns')
    curValue.name = 'Current Value'
    
    # Add the current value of open currencies
    overview_valued_df = overview_nomed_df.join(curValue,on='targetCurrency')
    overview_valued_df['PNL'] += overview_valued_df['Current Value'].where(pd.isna(overview_nomed_df['Close Time']), 0)
    overview_valued_df.drop(columns=['Current Value'], inplace=True)
    
    # Sort by date to get our final table (newer / open trades at the end)
    result_df = overview_valued_df.sort_values(by=['Close Time', 'Open Time'], kind='stable')
    return result_df
