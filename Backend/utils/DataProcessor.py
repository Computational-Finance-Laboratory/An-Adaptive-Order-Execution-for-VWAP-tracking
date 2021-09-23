from datetime import datetime, time, timedelta
from dateutil.relativedelta import relativedelta

import pandas as pd
import os
import re
import pickle
import time

def calculate_best_side(row, side):
    """
    Calculate best price and volume of specific side
    row (series): bid or offer series
    side (str): side to determine best price and volume

    return
    best_price (float): best price of specific side
    best_volume (float): best volume of specific side
    """
    filter_row = row[ row != 0 ]
    if filter_row.empty:
        best_price = None
        best_volume = None
    else:
        if side.lower() == "bid":
            best_price = filter_row.index[-1]
            best_volume = filter_row[-1]
        elif side.lower() == "offer":
            best_price = filter_row.index[0]
            best_volume = filter_row[0]
    return best_price, best_volume

def calculate_best_side_list(df, side):
    """
    Calculate list of best price and volume of specific side
    df (series): dataframe contained bid or offer at several time
    side (str): side to determine best price and volume

    return
    best_price_list (float): list of best price of specific side
    best_volume_list (float): list of best volume of specific side
    """
    best_price_list = []
    best_volume_list = []
    df = drop_auction_hour_columns(df)
    for i in range(df.shape[0]):
        best_price, best_volume = calculate_best_side(df.iloc[i], side=side)
        best_price_list.append(float(best_price) if best_price else best_price)
        best_volume_list.append(best_volume)
    return best_price_list, best_volume_list

def filter_auction_hour_with_trade(df, trade_df):
    """
    Filter out auction trade with trade data

    df (df): dataframe
    trade_df (df): dataframe with trade datas

    return (df): filtered dataframe
    """
    # Use one more index to skip auction trade
    first_trade_after_break = trade_df[ trade_df.index.time > time(14, 0, 0) ].index[1]
    df = df[ 
        ((df.index > trade_df.index[0]) & (df.index.time <= time(13, 0, 0))) |
        ((df.index >= first_trade_after_break) & (df.index <= trade_df.index[-2]))
    ]
    df = df[ df.index < trade_df[ trade_df.index.time <= time(16, 30, 0) ].index[-1] ]
    return df

def filter_auction_hour(df, over_filter_min=0):
    """
    Filter out auction hour with fixed time

    df (df): dataframe
    over_filter_min (int): amount of minute to additionally filter out the time after and before auction hour

    return (df): dataframe without data in auction hour
    """
    return df[ ((df.index.time >= time(10, 0 + over_filter_min, 0)) & (df.index.time <= time(12, 30 - over_filter_min, 0))) | 
          ((df.index.time >= time(14, 30 + over_filter_min, 0)) & (df.index.time <= time(16, 30 - over_filter_min, 0)))]

def filter_trade_auction_hour(df):
    """
    Filter out auction hour from trade dataframe

    df (df): trade dataframe

    return (df): trade dataframe without trading in auction hour
    """
    df = df.iloc[1:-1] # filter ATO, ATC hour
    after_break_df = df[df.index.time >= time(14, 0, 0)]
    if not after_break_df.empty:
        df = df.drop(after_break_df.index[0], axis=0) # filter ATO after break
    return df

def filter_break_hour(df):
    """
    Filter out break hour 

    df (df): dataframe

    return (df): dataframe without data in break hour
    """
    return df[ (df.index.time <= time(12, 30, 0)) |
                (df.index.time >= time(14, 30, 0)) ]


def fix_wrong_time(df):
    """
    Fix wrong time data in bid offer file by changing wrong time to be lastest time plus 1 microsecond

    df (df): bid or offer dataframe

    return (df): fixed dataframe
    """
    df = df.copy()
    new_time_list = [df.index[0]]
    for i in range(1, df.shape[0]):
        if df.index[i] < new_time_list[i-1]:
            new_time_list.append(new_time_list[i-1] + relativedelta(microseconds=1))
        else:
            new_time_list.append(df.index[i])
    df["Time"] = new_time_list
    df = df.set_index("Time")
    return df

def drop_auction_hour_columns(df):
    """
    Drop columns that relate to auction hour 

    df (df): bid or offere dataframe

    return (df): dropped dataframe`
    """
    df = df.copy()
    if df.columns.isin(["MARKET_BID"]).any():
        df = df.drop(["MARKET_BID"], axis=1)
    if df.columns.isin(["MARKET_OFFER"]).any():
        df = df.drop(["MARKET_OFFER"], axis=1)
    return df

def calculate_trade_side(trade_df, bid_df):
    """
    Calculate trade side from trade dataframe and bid dataframe

    trade_df (df): trade dataframe
    bid_df (df): bid dataframe
    
    return (list): list of side
    """
    side_list = []
    for i in range(len(trade_df)):
        price = trade_df["Prices"].iloc[i]
        timestamp = trade_df.index[i]
        bid_book = bid_df[:timestamp][:-1]
        # offer_book = offer_df[:timestamp][:-1]
        if len(bid_book) == 0:
            bid_book = bid_book[:timestamp]
            # offer_book = offer_book[:timestamp]
        if str(price) in bid_book and bid_book.iloc[-1][str(price)] > 0: # ATC and ATO price sometime get price that is not right spread
            side_list.append("S")
        else:
            side_list.append("B")
    return side_list

def calculate_fake_bid_offer(bid_df, offer_df, trade_df):
    """
    Calculate bid and offer changing without matching from dataframe

    bid_df (df): bid dataframe
    offer_df (df): offer dataframe
    trade_df (df): trade dataframe with ***side***

    return 
    fake_bid_df (df): bid changing dataframe without trade matching
    fake_offer_df (df): offer changingdataframe without trade matching
    """
    fake_bid_df = bid_df.diff()
    fake_offer_df = offer_df.diff()
    for i in range(trade_df.shape[0]):
        time = trade_df.index[i]
        price = trade_df["Prices"].iloc[i]
        volume = trade_df["Volumes"].iloc[i]
        side = trade_df["side"].iloc[i]
        price = str(price)
        
        if side == "B":
            if time in fake_offer_df.index:
                fake_offer_df[price].loc[time] += volume
        elif side == "S":
            if time in fake_bid_df.index:
                fake_bid_df[price].loc[time] += volume

    return fake_bid_df, fake_offer_df

def check_symbol_type(symbol):
    """
    Check type of symbol contained number

    symbol (str): symbol name contained number

    return (str): type of symbol
    """
    if "-" in symbol:
        return "warrant"
    elif len(symbol) <= 10:
        return "stock"
    else:
        return "unknown"

def get_first_symbol_number(symbol_list):
    """
    Return only first number of each symbol (filter to get only common stock)

    symbol_list (list): list of symbol contained all stock number

    return (list): list of common stock symbol
    """
    common_stock_list = []
    symbol_list = sorted(symbol_list)
    last_symbol_name = symbol_list[0][:-4]
    for symbol in symbol_list[1:]:
        symbol_name = symbol[:-4]
        if symbol_name != last_symbol_name:
            common_stock_list.append(symbol)
        last_symbol_name = symbol_name
    return common_stock_list            
        

def set_df_datetime(df, date_time):
    """
    set datetime to df 

    df (dataframe) : usually have only time
    date_time (datetime): use to set a base date

    return df
    """
    dt_list = []
    df = df.copy()
    
    for i in range(df.shape[0]):
        date_time = date_time.replace(hour=0, minute=0, second=0)
        date_time += timedelta(hours=df.index[i].hour, minutes=df.index[i].minute)
        dt_list.append(date_time)

    df["Time"] = dt_list
    df = df.set_index("Time")
    return df

def match_index(list1, list2):
    """
    match index that contains the same value (list1 should has smaller size than list2)

    return a list where index [i] represent index of list1, value[i] represent an index of list2 that has the same value
    """
    index = []
    count = 0
    for i in range(len(list1)):
        for j in range(count, len(list2)):
            if list1[i] == list2[j]:
                index.append(j)
        count += 1
    return index

def create_table_row(time, volume, price, lomo):
    """
    create a row for output box's table

    time (str) : a string of time in format %H:%M (example : 10:30)
    volume (str) : a volume from model output
    price (str) : a price that match from data
    lomo (str) : Order Type, either LO or MO

    return row
    """
    row = {
        "time": time.strftime('%H:%M'),
        "volume": str(volume),
        "price": str(format(price, ".2f")),
        "otype": lomo
    }
    return row

def avg_cal(table):
    """
    calculate average vwap of model output

    table (list of rows) : table from model_output_to_table

    return avg (float)
    """
    sum_pv = 0
    sum_vol = 0
    for row in table:
        sum_pv += float(row["price"]) * float(row["volume"])
        sum_vol += float(row["volume"])
    avg = sum_pv / sum_vol
    return avg


def change_table_format(table):
    """
    format a number in Volume from xxxxxxx.xx to x,xxx,xxx.xx

    table : a table from model_output_to_table

    return formatted table
    """
    for i in range(len(table)):
        table[i]["volume"] = format(int(table[i]["volume"]), ",")
    return table

def change_date_format(working_date):
    """
    change date format from YYYY-MM-DD to YYYYMMDD

    working_date (date)

    return date
    """
    date_time = datetime.strptime(working_date, '%Y-%m-%d')
    date = date_time.strftime("%Y%m%d")
    return date

def convert_pandas_to_string_csv(df):
    """
    change dataframe to string csv 

    df : dataframe of table

    return string csv 
    """
    return ",".join(list(df.columns)) + "\n" + "\n".join([ ",".join(list(df.iloc[i])) for i in range(df.shape[0])])

def str_csv_format(symbol,date,side,order_volume,avg,vwap,diff,table):
    """
    create string csv for download

    symbol (str) : symbol name
    date (str) : date of input
    side (str) : buy or sell
    order_volume (int) : volume of input
    avg (float) : average vwap 
    vwap (float) : market vwap
    diff (float) : difference of average vwap and market vwap
    table : a table from model_output_to_table

    return string csv 
    """
    ticker = 'Ticker,' + symbol
    date = 'Date,' + date
    side = 'Side,' + side
    order_volume = 'Order Volume,' + str(order_volume)
    no_of_order = 'Number of Order,' + str(len(table))
    avg = 'AVG,' + str(avg)
    market_vwap = 'Market VWAP,' + str(vwap)
    diff = 'Diff,' + str(diff)
    table = convert_pandas_to_string_csv(pd.DataFrame.from_records(table))
    return ticker + "\n" + date + "\n" + side + "\n" + order_volume + "\n" + no_of_order + "\n" + avg + "\n" + market_vwap + "\n" + diff + "\n" + table
