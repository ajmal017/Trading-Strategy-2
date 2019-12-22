# -*- coding: utf-8 -*
#!/usr/bin/env python3

"""
The project is to implement a distributed Python platform that could be used to test a quant models for trading
financial instruments in a network setting under client/server infrastructure. The client side including:
1. Market data feed
2. Build Model
3. Back testing
4. Simulated Trading
4. Web Dashboard Implementation

The server is a multi-thread application which coordinates among all the client applications. Its main purposes are
(1) messaging among all the participants in json format,
(2) maintain a market participant list, and the list of stocks traded by participants,
(3) generate a consolidated order book for all the participants, and 4) create a simulated market for 30 days based on last 3 months of market data. The sever platform will be mostly done by professor, by incorporating your inputs, feedbacks and requests.

"""


import json
import datetime as dt

import urllib.request
import pandas as pd
import numpy as np
import sqlite3
from sqlite3 import Error
import math

from datetime import timedelta,datetime
import pytz
import tzlocal
from pytz import timezone
from sqlalchemy import Column, ForeignKey, Integer, Float, String
from sqlalchemy import create_engine
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy import inspect
from sqlalchemy import and_, or_, not_

from flask import Flask, flash, redirect, render_template, request, url_for
from socket import AF_INET, socket, SOCK_STREAM
from enum import Enum
import struct

import sys
import queue
import threading
import random
import time

app = Flask(__name__) #webinterface

# A CSV file has S&P 500 Stock tickers
location_of_stocks = 'csv/10stock.csv'

engine = create_engine('sqlite:///my_data.db')
conn = engine.connect()
conn.execute("PRAGMA foreign_keys = ON")

# MetaData is a container object that keeps together many different features of a database 
metadata = MetaData()
metadata.reflect(bind=engine)

start_date = dt.date(2019, 8, 1)
end_date = dt.datetime.now()

if(len(sys.argv) > 1) :
    clientID = sys.argv[1]
else:
    clientID = "SZ"

#HOST = "10.18.217.199"
HOST = "192.168.1.202"
PORT = 6510

BUFSIZ = 4096
ADDR = (HOST, PORT)

client_socket = socket(AF_INET, SOCK_STREAM)
client_socket.connect(ADDR)

#my_symbols = "AAL,AAPL,CNC,FB,INTC,MDT,MU,NFLX,NVDA,SO"
my_symbols="INTC,MU,CNC,MDT"
symbol_list=list(my_symbols.split(','))


q = queue.Queue()
e = threading.Event()

bTradeComplete = False
orders = []

class PacketTypes(Enum):
    CONNECTION_NONE = 0
    CONNECTION_REQ = 1
    CONNECTION_RSP = 2
    CLIENT_LIST_REQ = 3
    CLIENT_LIST_RSP = 4
    STOCK_LIST_REQ = 5
    STOCK_LIST_RSP = 6
    STOCK_REQ = 7
    STOCK_RSP = 8
    BOOK_INQUIRY_REQ = 9
    BOOK_INQUIRY_RSP = 10
    NEW_ORDER_REQ = 11
    NEW_ORDER_RSP = 12
    MARKET_STATUS_REQ = 13
    MARKET_STATUS_RSP = 14
    END_REQ = 15
    END_RSP = 16
    SERVER_DOWN_REQ = 17
    SERVER_DOWN_RSP = 18


class Packet:
    def __init__(self):
        self.m_type = 0
        self.m_msg_size = 0
        self.m_data_size = 0
        self.m_data = ""

    def __str__(self):
        return str(self.__class__) + ": " + str(self.__dict__) + "\n"

    def __repr__(self):
        return str(self.__class__) + ": " + str(self.__dict__) + "\n"

    def serialize(self):
        self.m_data_size = 12 + len(self.m_data)
        self.m_msg_size = self.m_data_size
        return self.m_type.to_bytes(4, byteorder='little') + \
               self.m_msg_size.to_bytes(4, byteorder='little') + \
               self.m_data_size.to_bytes(4, byteorder='little') + \
               bytes(self.m_data, 'utf-8')

    def deserialize(self, message):
        msg_len = len(message)
        msg_unpack_string = '<iii' + str(msg_len - 12) + 's'
        self.m_type, self.m_msg_size, self.m_data_size, msg_data = struct.unpack(msg_unpack_string, message)
        # print("m_msg_size = ", self.m_msg_size, "m_data_size = ", self.m_data_size,)
        # print("self.msg_data:", msg_data[0:self.m_data_size-1], "msg_data", msg_data)
        self.m_data = msg_data[0:self.m_data_size - 12].decode('utf-8')
        return message[self.m_data_size:]

# Get 1-month stock daily data from 08/01/2019.
def get_daily_data(symbol, 
                   start=start_date, 
                   end=end_date, 
                   requestURL='https://eodhistoricaldata.com/api/eod/',
                   apiKey='5ba84ea974ab42.45160048'):
    symbolURL = str(symbol) + '.US?'
    startURL = 'from=' + str(start)
    endURL = 'to=' + str(end)
    apiKeyURL = 'api_token=' + apiKey
    completeURL = requestURL + symbolURL + startURL + '&' + endURL + '&' + apiKeyURL + '&period=d&fmt=json'
    print(completeURL)
    with urllib.request.urlopen(completeURL) as req:
        data = json.load(req)
        return data


# Get 1-month stock intraday data from 08/01/2019.
def get_intraday_data(symbol,
                      requestURL = 'https://eodhistoricaldata.com/api/intraday/',
                      apiKey = '5ba84ea974ab42.45160048'):
    symbolURL = str(symbol) + '.US?'
    apiKeyURL = 'api_token=' + apiKey
    completeURL = requestURL + symbolURL + apiKeyURL + '&interval=5m&fmt=json'
    print(completeURL)
    with urllib.request.urlopen(completeURL) as req:
        data = json.load(req)
        return data


def get_shares_data(symbol,
                         requestURL='https://eodhistoricaldata.com/api/fundamentals/',
                         apiKey='5ba84ea974ab42.45160048'):
    symbolURL = str(symbol) + '.US?'
    apiKeyURL = 'api_token=' + apiKey
    completeURL = requestURL + symbolURL + apiKeyURL +'&filter=SharesStats::SharesOutstanding'
    print(completeURL)
    with urllib.request.urlopen(completeURL) as req:
        data = json.load(req)
        #print(data)
        return data

def get_pb_data(symbol,
                         requestURL='https://eodhistoricaldata.com/api/fundamentals/',
                         apiKey='5ba84ea974ab42.45160048'):
    symbolURL = str(symbol) + '.US?'
    apiKeyURL = 'api_token=' + apiKey
    completeURL = requestURL + symbolURL + apiKeyURL +'&filter=Valuation::PriceBookMRQ'
    print(completeURL)
    with urllib.request.urlopen(completeURL) as req:
        data = json.load(req)
        #print(data)
        return data

def get_roe_data(symbol,
                         requestURL='https://eodhistoricaldata.com/api/fundamentals/',
                         apiKey='5ba84ea974ab42.45160048'):
    symbolURL = str(symbol) + '.US?'
    apiKeyURL = 'api_token=' + apiKey
    completeURL = requestURL + symbolURL + apiKeyURL +'&filter=Highlights::ReturnOnEquityTTM'
    print(completeURL)
    with urllib.request.urlopen(completeURL) as req:
        data = json.load(req)
        #print(data)
        return data

def create_stocks_table(table_name, metadata, engine):
    table = Table(table_name, metadata,
                  Column('Ticker', String(50), primary_key=True, nullable=False),
                  Column('Profit_Loss', Float, nullable=False),
                  extend_existing=True)
    table.create(engine)

# Create a daily price table to store all historical stock data.
def create_price_table(table_name, metadata, engine):
    tables = metadata.tables.keys()
    if table_name not in tables:
        table = Table(table_name, metadata,
                  Column('Symbol', String(50), ForeignKey('SP500.Ticker'), primary_key=True, nullable=False),
                  Column('Date', String(50), primary_key=True, nullable=False),
                  Column('Open', Float, nullable=False),
                  Column('High', Float, nullable=False),
                  Column('Low', Float, nullable=False),
                  Column('Close', Float, nullable=False),
                  Column('Adjusted_close', Float, nullable=False),
                  Column('Volume', Integer, nullable=False))
        table.create(engine)


# Create intraday data table
def create_intraday_price_table(table_name, metadata, engine):
    tables=metadata.tables.keys()
    if table_name not in tables:
        table = Table(table_name, metadata,
                      Column('Symbol', String(50), ForeignKey('SP500.Ticker'), primary_key=True, nullable=False),
                      Column('Date', String(50), primary_key=True, nullable=False),
                      Column('Time', String(50), primary_key=True, nullable=False),
                      Column('Open', Float, nullable=True),
                      Column('High', Float, nullable=True),
                      Column('Low', Float, nullable=True),
                      Column('Close', Float, nullable=True),
                      Column('Adjusted_close', Float, nullable=True),
                      Column('Volume', Integer, nullable=True))
        table.create(engine)


def create_outstanding_shares_table(table_name, metadata, engine):
    tables = metadata.tables.keys()
    if table_name not in tables:
        table = Table(table_name, metadata,
                      Column('Symbol', String(50), ForeignKey('SP500.Ticker'), primary_key=True, nullable=False),
                      Column('Outstanding_shares', Float, nullable=True))
        table.create(engine)


# Create a R-BREAKER table to calculate seven signal points based on high, low, close each day.
def create_r_breaker_table(table_name, metadata, engine):
    table = Table(table_name, metadata,
                Column('Symbol', String(50), ForeignKey('Intraday_Data.Symbol'), primary_key=True, nullable=False),
                Column('Date', String(50), ForeignKey('Daily_Data.Date'), primary_key=True, nullable=False),
                Column('PivotPoint', Float, nullable=False),
                Column('TrendBuy', Float, nullable=False),
                Column('SellCheck', Float, nullable=False),
                Column('RevSell', Float, nullable=False),
                Column('RevBuy', Float, nullable=False),
                Column('BuyCheck', Float, nullable=False),
                Column('TrendSell', Float, nullable=False),
                extend_existing=True)
    table.create(engine)


# Create  ATR indicator table to calculate N-day ATR based on close price of each stock.
# N needs to be determined, default value is 14 days.
def create_ATR_table(table_name, metadata, engine):
    table = Table(table_name, metadata,
                Column('Symbol', String(50), ForeignKey('Daily_Data.Symbol'), primary_key=True, nullable=False),
                Column('Date', String(50), ForeignKey('Daily_Data.Date'), primary_key=True, nullable=False),
                Column('ATR', Float, nullable=True),
                extend_existing=True)
    table.create(engine)


# Create average volume table to calculate 30-day average volume for each stock.
def create_avg_volume_table(table_name, metadata, engine):
    table = Table(table_name, metadata,
                  Column('Symbol', String(50), ForeignKey('Daily_Data.Symbol'), primary_key=True, nullable=False),
                  Column('Date', String(50), ForeignKey('Daily_Data.Date'), primary_key=True, nullable=False),
                  Column('Average Volume', Float, nullable=True),
                  extend_existing=True)
    table.create(engine)

# SQLITE table to store trading information
def create_trade_table(table_name, metadata, engine):
        table = Table(table_name, metadata,
                      Column('Symbol', String(50), primary_key=True, nullable=False),
                      Column('Date', String(50), primary_key=True, nullable=False),
                      Column('Type', String(50), primary_key=True, nullable=False),
                      Column('OpenTime', String(50), primary_key=False, nullable=False),
                      Column('OpenPrice', Float, nullable=False),
                      Column('CloseTime', String(50), nullable=False),
                      Column('ClosePrice', Float, nullable=False),
                      Column('Qty', Integer, nullable=False),
                      Column('Profit_Loss', Float, nullable=False),
                      extend_existing=True)
        table.create(engine)

def clear_a_table(table_name, metadata, engine):
    conn = engine.connect()
    table = metadata.tables[table_name]
    delete_st = table.delete()
    conn.execute(delete_st)

def execute_sql_statement(sql_st, engine):
    result = engine.execute(sql_st)
    return result


# Populate stock daily data into stock price table
def populate_stock_data(tickers, engine, table_name, start_date, end_date):
    column_names = ['Symbol', 'Date', 'Open', 'High', 'Low', 'Close', 'Adjusted_close', 'Volume']
    price_data = []
    for ticker in tickers:
        stock = get_daily_data(ticker, start_date, end_date)
        for stock_data in stock:
            price_data.append([ticker, stock_data['date'], stock_data['open'], stock_data['high'], stock_data['low'], \
                               stock_data['close'], stock_data['adjusted_close'], stock_data['volume']])
        print(price_data)
    stocks = pd.DataFrame(price_data, columns=column_names)
    stocks.to_sql(table_name, con=engine, if_exists='append', index=False)


# Populate stock intraday data into stock price table
def populate_intraday_stock_data(tickers, engine, table_name):
    column_names = ['Symbol', 'Date', 'Time', 'Open', 'High', 'Low', 'Close', 'Adjusted_close', 'Volume']
    price_data = []
    for ticker in tickers:
        stock = get_intraday_data(ticker)
        for stock_data in stock:
            split = dt.datetime.strptime(stock_data['datetime'], '%Y-%m-%d %H:%M:%S')
            split_local=split.replace(tzinfo=pytz.utc).astimezone(tzlocal.get_localzone())
            price_data.append([ticker, split_local.date(), split_local.time(), stock_data['open'], stock_data['high'], stock_data['low'], \
                               stock_data['close'], stock_data['adjusted_close'], stock_data['volume']])
        print(price_data)
    stocks = pd.DataFrame(price_data, columns=column_names)
    #stocks[['Date', 'Time']] = stocks.Datetime.str.split(expand=True)
    stocks.to_sql(table_name, con=engine, if_exists='append', index=False)


def populate_outstanding_shares_data(tickers, engine, table_name):
    column_names = ['Symbol', 'Outstanding_shares']
    data = []
    for ticker in tickers:
        shares = get_shares_data(ticker)
        data.append([ticker, shares])
        #print(data)
    stocks = pd.DataFrame(data, columns=column_names)
    stocks.to_sql(table_name, con=engine, if_exists='append', index=False)
    stocks.to_csv(r'outstanding_shares.csv', index=False)

def populate_PB_data(tickers, engine):
    column_names = ['Symbol', 'Price/Book Ratio']
    data = []
    for ticker in tickers:
        ratio = get_pb_data(ticker)
        data.append([ticker, ratio])
    stocks = pd.DataFrame(data, columns=column_names)
    #stocks.to_sql(table_name, con=engine, if_exists='append', index=False)
    #stocks.to_csv(r'PriceBookRatio.csv', index=False)

def populate_ROE_data(tickers, engine):
    column_names = ['Symbol', 'ROE Ratio']
    data = []
    for ticker in tickers:
        ratio = get_roe_data(ticker)
        data.append([ticker, ratio])
    stocks = pd.DataFrame(data, columns=column_names)
    #stocks.to_sql(table_name, con=engine, if_exists='append', index=False)
    stocks.to_csv(r'ROERatio.csv', index=False)

# get_data from SQLITE3
def get_data(db_file, table_name, select = '*'):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except Error as e:
        print(e)
    query = 'SELECT {} FROM {}'.format(select, str(table_name))
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    col_names = list(map(lambda x: x[0], cur.description))
    df = pd.DataFrame(rows, columns = col_names)
    return df

# Calculate r_breaker signal using stock daily data
def populate_r_breaker_data(table_name, engine):
    df = get_data('market data.db', 'Daily_Data')
    df['PivotPoint'] = (df['High'] + df['Low'] + df['Close'])/3
    df['SellCheck'] = df['High'] + 0.33 * (df['Close'] - df['Low'])
    df['RevSell'] = (1 + 0.07)/2 * (df['High'] + df['Low']) - 0.07 * df['Low']
    df['RevBuy'] = (1 + 0.07)/2 * (df['High'] + df['Low']) - 0.07 * df['High']
    df['BuyCheck'] = df['Low'] - 0.33 * (df['High'] - df['Close'])
    df['TrendBuy'] = df['SellCheck'] + 0.24 * (df['SellCheck'] - df['BuyCheck'])
    df['TrendSell'] = df['BuyCheck'] - 0.24 * (df['SellCheck'] - df['BuyCheck'])
    df_1 = df.iloc[:, 0:2]
    df_2 = df.iloc[:, -7:]
    df_1 = df_1.join(df_2)
    df_1.to_sql(table_name, con=engine, if_exists='append', index=False)
    print(df)


# Calculate average volume signal using stock daily data
def populate_average_volume(table_name, engine):
    df = get_data('market data.db', 'Daily_Data')
    df['Average Volume'] = df['Volume'].rolling(window=30).mean()
    df = df.loc[:, ['Symbol', 'Date', 'Average Volume']]
    df.to_sql(table_name, con=engine, if_exists='append', index=False)
    df.to_csv(r'average_volume.csv', index=False)
    print(df)


# Calculate ATR signal using stock daily data
def populate_ATR(table_name, engine):
    df = get_data('market data.db', 'Daily_Data')
    for i in range(len(df)):
          value1 = df.loc[i, 'High']-df.loc[i, 'Low']
          value2 = abs(df.loc[i, 'High'] - df.loc[i, 'Close'])
          value3 = abs(df.loc[i, 'Low'] - df.loc[i, 'Close'])
          df.loc[i, 'TR'] = max(value1, value2, value3)
    df['ATR'] = (df['TR'].shift(1).rolling(window=13).sum()+df['TR'])/14
    df = df.loc[:, ['Symbol', 'Date', 'ATR']]
    df.to_sql(table_name, con=engine, if_exists='append', index=False)
    print(df)

def create_open_position_table(table_name, metadata, engine):
        table = Table(table_name, metadata,
                      Column('Symbol', String(50), primary_key=True, nullable=False),
                      Column('Date', String(50), primary_key=True, nullable=False),
                      Column('Time', String(50), primary_key=True, nullable=False),
                      Column('Current_Price', Float, nullable=True),
                      Column('Close', Float, nullable=True),
                      Column('Open', Float, nullable=True),
                      Column('High', Float, nullable=True),
                      Column('Low', Float, nullable=True),
                      Column('TrendBuy', Float, nullable=True),
                      Column('TrendSell', Float, nullable=True),
                      Column('BuyCheck', Float, nullable=True),
                      Column('SellCheck', Float, nullable=True),
                      Column('RevBuy', Float, nullable=True),
                      Column('RevSell', Float, nullable=True),
                      Column('ATR', Float, nullable=True),
                      extend_existing=True)
        table.create(engine)

def build_trading_model():
    engine.execute('Drop Table if exists SP500;')
    create_stocks_table('SP500', metadata, engine)
    
    lists = pd.read_csv(location_of_stocks)
    lists["Profit_Loss"] = 0.0
    lists.to_sql('SP500', con=engine, if_exists='append', index=False)

    create_price_table('Daily_Data', metadata, engine)
    inspector = inspect(engine)
    print(inspector.get_table_names())
    clear_a_table('Daily_Data', metadata, engine)
    populate_stock_data(lists['Ticker'].unique(), engine, 'Daily_Data', start_date, end_date)

    create_intraday_price_table('Intraday_Data', metadata, engine)
    inspector = inspect(engine)
    print(inspector.get_table_names())
    clear_a_table('Intraday_Data', metadata, engine)
    populate_intraday_stock_data(lists['Ticker'], engine, 'Intraday_Data')

    engine.execute('Drop Table if exists R_breaker;')
    create_r_breaker_table('R_breaker', metadata, engine)
    populate_r_breaker_data('R_breaker', engine)

    engine.execute('Drop Table if exists Average_Volume')
    create_avg_volume_table('Average_Volume', metadata, engine)
    populate_average_volume('Average_Volume', engine)

    engine.execute('Drop Table if exists ATR_Indicator')
    create_ATR_table('ATR_Indicator', metadata, engine)
    populate_ATR('ATR_Indicator', engine)

    engine.execute('Drop Table if exists Outstanding_shares')
    create_outstanding_shares_table('Outstanding_shares', metadata, engine)
    populate_outstanding_shares_data(lists['Ticker'], engine, 'Outstanding_shares')



    select_st = "Select SP500.Ticker as Symbol,\
                        Intraday_Data.Date as Date, Intraday_Data.Time as Time,\
                        Intraday_Data.Close as Current_price, Daily_Data.Close as Close, Daily_Data.Open as Open, \
                        Daily_Data.High as High, Daily_Data.Low as Low, \
                        R_breaker.TrendBuy, R_breaker.TrendSell, R_breaker.BuyCheck, R_breaker.SellCheck, \
                        R_breaker.RevSell, R_breaker.RevBuy, ATR_Indicator.ATR \
                        From Intraday_Data, R_breaker, ATR_Indicator,SP500, Daily_Data \
                        Where (Intraday_Data.Date = R_breaker.Date and Intraday_Data.Date = ATR_Indicator.Date and \
                        Intraday_Data.Date=Daily_Data.Date and SP500.Ticker = Intraday_Data.Symbol and \
                        SP500.Ticker = R_breaker.Symbol and SP500.Ticker =ATR_Indicator.Symbol and \
                        SP500.Ticker = Daily_Data.Symbol);"


    engine.execute('Drop Table if exists Open_Position;')
    create_open_position_table('Open_Position', metadata, engine)

    result_set = execute_sql_statement(select_st, engine)
    result_df = pd.DataFrame(result_set.fetchall())
    result_df.columns = result_set.keys()

    result_df['TrendBuy'] = result_df.groupby(['Symbol'])['TrendBuy'].shift(78)
    result_df['TrendSell'] = result_df.groupby(['Symbol'])['TrendSell'].shift(78)
    result_df['BuyCheck']= result_df.groupby(['Symbol'])['BuyCheck'].shift(78)
    result_df['SellCheck'] = result_df.groupby(['Symbol'])['SellCheck'].shift(78)
    result_df['RevBuy'] = result_df.groupby(['Symbol'])['RevBuy'].shift(78)
    result_df['RevSell'] = result_df.groupby(['Symbol'])['RevSell'].shift(78)
    result_df['ATR']= result_df.groupby(['Symbol'])['ATR'].shift(78)

    result_df.to_sql('Open_Position', con=engine, if_exists='append', index=False)


class Stock:
        # self is reference
        def __init__(self, symbol, date):
            self.ticker = symbol
            self.date = date
            self.signal = {}
            self.total_profit_loss = 0.0  # for every stock+date

        def __str__(self):
            return str(self.__class__) + ": " + str(self.__dict__) + "\n"  # cout overloading debug

        def __repr__(self):
            return str(self.__class__) + ": " + str(self.__dict__) + "\n"

        # read signal table in SQLITE3 and assign parameters value to stock class object
        def createsignal(self, time, current_price, close, open, trendbuy, trendsell, ATR, revbuy, revsell, buycheck,
                         sellcheck, high, low, qty1=0, qty2=0):
            self.signal[time] = np.array(
                [time, current_price, close, open, trendbuy, trendsell, ATR, revbuy, revsell, buycheck, sellcheck, high,
                 low, qty1, qty2])

        # implement trading strategy
        def updateTrades(self):
            signal_matrix = np.array(list(self.signal.values()))
            signal = pd.DataFrame(signal_matrix).reset_index(drop=True)

            temp1 = signal.loc[:, 1:].astype(float)
            time = signal.loc[:, 0]
            signal = pd.concat([pd.DataFrame(time), temp1], axis=1)
            trades = [None] * 7

            for index in range(0, len(signal) - 1):
                if (signal.iloc[index, 1] > signal.iloc[index, 4]) | (
                        signal.iloc[index, 1] > (signal.iloc[index, 3] + 0.5 * signal.iloc[index, 6])) \
                        & (signal.iloc[:index - 1, 13].sum() == 0) & (signal.iloc[:index - 1, 14].sum() == 0):

                    #signal.iloc[index, 13] = int(math.floor(cash * 0.01 / signal.iloc[index, 6] / 200.0)) * 100
                    signal.iloc[index, 13] = 1000
                    trades[0] = 'Long'
                    trades[1] = signal.iloc[index, 0]
                    trades[2] = signal.iloc[index, 1]
                    trades[5] = 1000

                    for j in range(index + 1, len(signal)):

                        if (signal.iloc[j, 1] > trades[2] + signal.iloc[index, 6]) or (
                                signal.iloc[j, 1] > trades[2] * (1 + 0.001)):
                            trades[3] = signal.iloc[j, 0]
                            trades[4] = signal.iloc[j, 1]
                            break
                        elif (signal.iloc[j, 1] < signal.iloc[j, 8]) & (signal.iloc[j, 11] > signal.iloc[j, 10]):
                            trades[3] = signal.iloc[j, 0]
                            trades[4] = signal.iloc[j, 1]
                            break
                        elif signal.iloc[j, 1] < trades[2] * (1 - 0.001):
                            trades[3] = signal.iloc[j, 0]
                            trades[4] = signal.iloc[j, 1]
                            break
                        else:
                            trades[3] = signal.iloc[-1, 0]
                            trades[4] = signal.iloc[-1, 1]
                            break

                elif (signal.iloc[index, 1] < signal.iloc[index, 5]) | (
                        signal.iloc[index, 1] > (signal.iloc[index, 3] - 0.5 * signal.iloc[index, 6])) \
                        & (signal.iloc[:index - 1, 13].sum() == 0) & (signal.iloc[:index - 1, 14].sum() == 0):
                    signal.iloc[index, 14] = -1000
                    trades[0] = 'Short'
                    trades[1] = signal.iloc[index, 0]
                    trades[2] = signal.iloc[index, 1]
                    trades[5] = -1000

                    for j in range(index + 1, len(signal)):
                        if (signal.iloc[j, 1] < trades[2] - signal.iloc[index, 6]) or (
                                signal.iloc[j, 1] < trades[2] * (1 - 0.001)):
                            trades[3] = signal.iloc[j, 0]
                            trades[4] = signal.iloc[j, 1]
                            break
                        elif (signal.iloc[j, 1] > signal.iloc[j, 7]) & (signal.iloc[j, 12] < signal.iloc[j, 9]):
                            trades[3] = signal.iloc[j, 0]
                            trades[4] = signal.iloc[j, 1]
                            break
                        elif signal.iloc[j, 1] > trades[2] * (1 + 0.001):
                            trades[3] = signal.iloc[j, 0]
                            trades[4] = signal.iloc[j, 1]
                            break
                        else:
                            trades[3] = signal.iloc[-1, 0]
                            trades[4] = signal.iloc[-1, 1]
                            break

            if all(element is None for element in trades) == False:

                if trades[0] == 'Long':
                    trades[6] = (trades[4] - trades[2]) * trades[5]
                elif trades[0] == 'Short':
                    trades[6] = (trades[2] - trades[4]) * trades[5]

                self.total_profit_loss += trades[6]
                print(trades)
                return trades


cash = 100000

def backtesting(metadata, engine):

        global trades_df
        engine.execute('Drop Table if exists Trades;')

        stock_map = dict()

        # Get Symbol + Date from database
        select_st = 'SELECT SP500.Ticker, Open_Position.Date FROM SP500 \
                     ,Open_Position WHERE SP500.Ticker=Open_Position.Symbol;'
        result_set = execute_sql_statement(select_st, engine)
        result_df = pd.DataFrame(result_set.fetchall())
        result_df.columns = result_set.keys()
        result_df = result_df.drop_duplicates().reset_index(drop=True)

        for index, row in result_df.iterrows():
            aKey = (row['Ticker'], row['Date'])
            stock_map[aKey] = Stock(row['Ticker'], row['Date'])
            print('str', aKey)

        # Get Price + Signal from database
        select_st = "Select * From Open_Position;"
        result_set = execute_sql_statement(select_st, engine)
        result_df = pd.DataFrame(result_set.fetchall())
        result_df.columns = result_set.keys()
        result_df = result_df.dropna(
            subset=['TrendBuy', 'TrendSell', 'BuyCheck', 'SellCheck', 'RevBuy', 'RevSell', 'ATR'],
            how='all').reset_index(drop=True)

        # Assign price and signal value to each stock object
        for index in range(0, result_df.shape[0]):
            aKey = (result_df.at[index, 'Symbol'], result_df.at[index, 'Date'])
            stock_map[aKey].createsignal(result_df.loc[index, 'Time'], result_df.loc[index, 'Current_Price'],
                                         result_df.loc[index, 'Close'], result_df.loc[index, 'Open'],
                                         result_df.loc[index, 'TrendBuy'], result_df.loc[index, 'TrendSell'],
                                         result_df.loc[index, 'ATR'],
                                         result_df.loc[index, 'RevBuy'], result_df.loc[index, 'RevSell'],
                                         result_df.loc[index, 'BuyCheck'],
                                         result_df.loc[index, 'SellCheck'], result_df.loc[index, 'High'],
                                         result_df.loc[index, 'Low'])

        i = 0
        key_table = pd.DataFrame(columns=['Symbol', 'Date'])
        trades_table = pd.DataFrame(
            columns=['Type', 'OpenTime', 'OpenPrice', 'CloseTime', 'ClosePrice', 'Qty', 'Profit_Loss'])

        # Trading strategy implementation
        for key, value in stock_map.items():
            key_table.loc[i, :] = list(key)
            trades_table.loc[i, :] = value.updateTrades()
            trades_df = pd.concat([key_table, trades_table], axis=1, sort=False)
            i += 1

            np.set_printoptions(precision=2, floatmode='fixed')
            np.set_printoptions(suppress=True)

        trades_df_new = trades_df.dropna(
            subset=['Type', 'OpenTime', 'OpenPrice', 'CloseTime', 'ClosePrice', 'Qty', 'Profit_Loss'],
            how='all').reset_index(drop=True)

        print(trades_df_new)
        print('Total Profit Loss:', trades_df_new['Profit_Loss'].sum())
        trades_df_new.to_csv(r'trades.csv', index=False)
        engine.execute('Drop Table if exists Trades;')
        create_trade_table('Trades', metadata, engine)
        trades_df_new.to_sql('Trades', con=engine, if_exists='append', index=False)

        update_st = """UPDATE SP500 SET Profit_Loss=(SELECT SUM(Trades.Profit_Loss) FROM Trades WHERE Trades.Symbol=SP500.Ticker);"""
        engine.execute(update_st)


def receive(q=None, e=None):
    """Handles receiving of messages."""
    total_server_response = b''

    msgSize = 0
    while True:
        try:
            server_response = client_socket.recv(BUFSIZ)
            if len(server_response) > 0:
                total_server_response += server_response
                # print(total_server_response)
                msgSize = len(total_server_response)
                while msgSize > 0:
                    if msgSize > 12:
                        server_packet = Packet()
                        server_response = server_packet.deserialize(total_server_response)
                        # print(server_packet.m_msg_size, msgSize, server_packet.m_data)
                    if msgSize > 12 and server_packet.m_data_size <= msgSize:
                        data = json.loads(server_packet.m_data)
                        # print(type(data), data)
                        q.put([server_packet.m_type, data])
                        if e.isSet():
                            e.clear()
                        total_server_response = total_server_response[server_packet.m_data_size:]
                        msgSize = len(total_server_response)
                        server_response = b''
                    else:
                        # break
                        server_response = client_socket.recv(BUFSIZ)
                        total_server_response += server_response
                        msgSize = len(total_server_response)
        except (OSError, Exception):
            # except (OSError,):
            q.put([PacketTypes.CONNECTION_NONE.value, Exception('receive')])
            print("Exception in receive\n")
            sys.exit(0)


def send(q=None):
    threading.Thread(target=receive, args=(q,)).start()
    try:
        while True:
            client_packet = Packet()
            user_input = input("Action:")
            input_list = user_input.strip().split(" ")
            if len(input_list) < 2:
                print("Incorrect Input.\n")
                continue
            if "Logon" in user_input:
                client_packet.m_type = PacketTypes.CONNECTION_REQ.value
                client_packet.m_data = json.dumps(
                    {'Client': clientID, 'Status': input_list[0], 'Symbol': input_list[1]})

            elif "Client List" in user_input:
                client_packet.m_type = PacketTypes.CLIENT_LIST_REQ.value
                client_packet.m_data = json.dumps({'Client': clientID, 'Status': input_list[0] + ' ' + input_list[1]})

            elif "Stock List" in user_input:
                client_packet.m_type = PacketTypes.STOCK_LIST_REQ.value
                client_packet.m_data = json.dumps({'Client': clientID, 'Status': input_list[0] + ' ' + input_list[1]})

            # elif "Stock" in user_input:
            #    input_list = user_input.split(" ")
            #    client_packet.m_type = PacketTypes.STOCK_REQ.value
            #    client_packet.m_data = json.dumps({'Client':clientID, 'Status':input_list[0],
            #                                     'Symbol':input_list[1],
            #                                     'Start':input_list[2],
            #                                     'End':input_list[3]})

            # elif "Market Status" in user_input:
            #    input_list = user_input.split(" ")
            #    client_packet.m_type = PacketTypes.MARKET_STATUS_REQ.value
            #    client_packet.m_data = json.dumps({'Client':clientID, 'Status':input_list[0]})

            elif "Book Inquiry" in user_input:
                if len(input_list) < 3:
                    print("Missing input item(s).\n")
                    continue
                client_packet.m_type = PacketTypes.BOOK_INQUIRY_REQ.value
                client_packet.m_data = json.dumps(
                    {'Client': clientID, 'Status': input_list[0] + ' ' + input_list[1], 'Symbol': input_list[2]})

            elif "New Order" in user_input:
                if len(input_list) < 6:
                    print("Missing input item(s).\n")
                    continue
                client_packet.m_type = PacketTypes.NEW_ORDER_REQ.value
                client_packet.m_data = json.dumps(
                    {'Client': clientID, 'Status': input_list[0] + ' ' + input_list[1], 'Symbol': input_list[2],
                     'Side': input_list[3], 'Price': input_list[4], 'Qty': input_list[5]})

            elif "Client Quit" in user_input:
                client_packet.m_type = PacketTypes.END_REQ.value
                client_packet.m_data = json.dumps({'Client': clientID, 'Status': input_list[0]})

            elif "Server Down" in user_input:
                client_packet.m_type = PacketTypes.SERVER_DOWN_REQ.value
                client_packet.m_data = json.dumps({'Client': clientID, 'Status': input_list[0] + ' ' + input_list[1]})

            else:
                print("Invalid message\n")
                continue

            client_socket.send(client_packet.serialize())
            data = json.loads(client_packet.m_data)
            print(data)

            msg_type, msg_data = q.get()
            q.task_done()
            print(msg_data)
            if msg_data is not None:
                if msg_type == PacketTypes.END_RSP.value or msg_type == PacketTypes.SERVER_DOWN_RSP.value or \
                        (msg_type == PacketTypes.CONNECTION_RSP.value and msg_data["Status"] == "Rejected"):
                    client_socket.close()
                    sys.exit(0)

    except(OSError, Exception):
        # except(OSError):
        q.put(PacketTypes.CONNECTION_NONE.value, Exception('send'))
        client_socket.close()
        sys.exit(0)

def logon(client_packet, symbols):
    client_packet.m_type = PacketTypes.CONNECTION_REQ.value
    client_packet.m_data = json.dumps({'Client': clientID, 'Status': 'Logon', 'Symbol': symbols})
    return client_packet


def get_client_list(client_packet):
    client_packet.m_type = PacketTypes.CLIENT_LIST_REQ.value
    client_packet.m_data = json.dumps({'Client': clientID, 'Status': 'Client List'})
    return client_packet


def get_stock_list(client_packet):
    client_packet.m_type = PacketTypes.STOCK_LIST_REQ.value
    client_packet.m_data = json.dumps({'Client': clientID, 'Status': 'Stock List'})
    return client_packet


def get_market_status(client_packet):
    client_packet.m_type = PacketTypes.MARKET_STATUS_REQ.value
    client_packet.m_data = json.dumps({'Client': clientID, 'Status': 'Market Status'})
    return client_packet

def get_order_book(client_packet, symbol):
    client_packet.m_type = PacketTypes.BOOK_INQUIRY_REQ.value
    client_packet.m_data = json.dumps({'Client':clientID, 'Status':'Book Inquiry', 'Symbol':symbol})
    return client_packet


def enter_a_new_order(client_packet, order_id, symbol, order_type, side, price, qty):
    if order_type == "Mkt":
        price = 0
    client_packet.m_type = PacketTypes.NEW_ORDER_REQ.value
    client_packet.m_data = json.dumps({'Client':clientID, 'OrderIndex':order_id, 'Status':'New Order', 'Symbol':symbol, 'Type':order_type, 'Side':side, 'Price':price, 'Qty':qty})
    return client_packet


def quit_connection(client_packet):
    client_packet.m_type = PacketTypes.END_REQ.value
    client_packet.m_data = json.dumps({'Client': clientID, 'Status': 'Client Quit'})
    return client_packet


def send_msg(client_packet):
    client_socket.send(client_packet.serialize())
    data = json.loads(client_packet.m_data)
    print(data)
    return data


def get_response(q):
    while (q.empty() == False):
        global msg_data
        msg_type, msg_data = q.get()
        #q.task_done()
        #print(msg_data)
        if msg_data is not None:
            if msg_type == PacketTypes.END_RSP.value or msg_type == PacketTypes.SERVER_DOWN_RSP.value or \
                (msg_type == PacketTypes.CONNECTION_RSP.value and msg_data["Status"] == "Rejected"):
                client_socket.close()
                sys.exit(0)
    return msg_data

def set_event(e):
    e.set();

def wait_for_an_event(e):
    while e.isSet():
        continue

def Get_Buy_Price(symbol, book, market_status):
    buy_price={}
    data={}
    buy_price.update({symbol:data})
    price=[]
    qty=[]

    for order in book:
        if (market_status == "Market Closed"):
            break
        else:
            if symbol == order['Symbol'] and order['Status'] == 'New' and order['Side']=='Sell':
                     price.append(order['Price'])
                     qty.append(order['Qty'])

    data.update({'Price': min(price)})
    data.update({'Qty':qty[price.index(min(price))]})
    return buy_price

def Get_Sell_Price(symbol, book, market_status):
    sell_price = {}
    data = {}
    sell_price.update({symbol: data})
    price=[]
    qty=[]
    for order in book:
        if (market_status == "Market Closed"):
            break
        else:
            if symbol == order['Symbol'] and order['Status'] == 'New' and order['Side'] == "Buy":
                 price.append(order['Price'])
                 qty.append(order['Qty'])

    data.update({'Price': max(price)})
    data.update({'Qty': qty[price.index(max(price))]})
    return sell_price


def join_trading_network(q, e):
    global bTradeComplete, my_position, my_price, my_qty, orders_record
    threading.Thread(target=receive, args=(q, e)).start()

    try:
        client_packet = Packet()
        global order_table
        set_event(e)
        send_msg(logon(client_packet, my_symbols))
        wait_for_an_event(e)
        print(get_response(q))

        set_event(e)
        send_msg(get_client_list(client_packet))
        wait_for_an_event(e)
        get_response(q)

        set_event(e)
        send_msg(get_stock_list(client_packet))
        wait_for_an_event(e)
        stock_data = get_response(q)
        # print(stock_data['Stock List'])
        # stock_list = list(stock_data['Stock List'].split(','))
        # print(stock_list)

        #stock_data['Stock List'] = 'AAL,AAPL,CNC,FB,INTC,MDT,MU,NFLX,NVDA,SO'
        stock_data['Stock List'] = 'INTC,MU,CNC,MDT'
        stock_list = list(stock_data['Stock List'].split(','))

        market_close_dict={}
        market_open_dict={}
        signal_dict={}
        trades_signal={}
        buy_results={}
        sell_results={}
        orders_record = {}
        OrderIndex = 0
        price_id = 0
        qty_id = 0

        while True:
            client_packet = Packet()
            set_event(e)
            send_msg(get_market_status(client_packet))
            wait_for_an_event(e)
            market_status_data = get_response(q)
            market_status = market_status_data["Status"]
            market_period = market_status_data["Market_Period"]

            client_packet = Packet()
            set_event(e)
            client_msg = get_order_book(client_packet, stock_data['Stock List'])
            send_msg(client_msg)
            wait_for_an_event(e)
            data = get_response(q)
            book_data = json.loads(data)
            order_book = book_data["data"]

            if market_period == "2019-12-10" :
                print("<<<<<<<<<<<<<<<<<<<<<<<Market Close>>>>>>>>>>>>>>>>>>>>>>>")
                break


            if market_status == "Open":
               print("=================Market Open=================")

               if bool(trades_signal)==False:
                       print("************Incomplete Signals************")
                       pass
               else:
                    if market_period not in market_open_dict.keys():
                                order_book_list = []
                                orders_list = []
                                orders_record.update({market_period: orders_list})
                                market_open_dict.update({market_period: order_book_list})

                    elif (market_period in market_open_dict.keys()) and (order_book not in order_book_list):
                                order_book_list.append(order_book)
                                market_open_dict.update({market_period: order_book_list})
                                last_date = list(trades_signal.keys())[-1]
                                signals = trades_signal[last_date]
                                buy_results.update({market_period: {}})
                                sell_results.update({market_period: {}})

                                while len(orders_record[market_period]) <= 4 :

                                    for symbol in stock_list:

                                        if (market_status == "Market Closed"):
                                            break

                                        value = signals[symbol]
                                        buy_price = Get_Buy_Price(symbol, order_book_list[-1],market_status)
                                        print("buy price")
                                        print(buy_price)

                                        sell_price=Get_Sell_Price(symbol,order_book_list[-1],market_status)
                                        print("sell price")
                                        print(sell_price)

                                        if (buy_price[symbol]['Price']>value['TrendBuy'])|(buy_price[symbol]['Price']>value['Close'] + 0.5*value['ATR']):

                                                    buy_price[symbol]['Position']='Long'
                                                    buy_results[market_period].update({symbol:buy_price[symbol]})
                                                    my_position="Buy"
                                                    my_price=buy_price[symbol]['Price']
                                                    my_qty=int(buy_price[symbol]['Qty']/100)

                                                    client_packet = Packet()
                                                    OrderIndex += 1
                                                    client_order_id = clientID + '_' + str(OrderIndex)+'_'+ market_period
                                                    print("send buy order")
                                                    enter_a_new_order(client_packet, client_order_id, symbol,
                                                                      'Lmt' if random.randint(1, 100) % 2 == 0 else 'Mkt', \
                                                                      side=my_position, price=my_price, qty=my_qty)
                                                    set_event(e)
                                                    send_msg(client_packet)
                                                    wait_for_an_event(e)
                                                    orders.append(get_response(q))

                                                    new_order=orders[-1]
                                                    if new_order['Status']!= "Order Reject":
                                                           new_order['Total Filled'] = 0
                                                           new_order['Profit_Loss'] = 0.0
                                                           orders_list.append(new_order)
                                                           orders_record.update({market_period: orders_list})
                                                    else:
                                                        pass

                                        elif (sell_price[symbol]['Price']<value['TrendSell'])|(sell_price[symbol]['Price']<value['Close'] - 0.5*value['ATR']):

                                                        sell_price[symbol]['Position']='Short'
                                                        sell_results[market_period].update({symbol:sell_price[symbol]})
                                                        my_position = "Sell"
                                                        my_price = buy_price[symbol]['Price']
                                                        my_qty = int(buy_price[symbol]['Qty'] / 100)

                                                        client_packet = Packet()
                                                        OrderIndex += 1
                                                        client_order_id = clientID + '_' + str(OrderIndex)+'_'+ market_period
                                                        print("send sell order")
                                                        enter_a_new_order(client_packet, client_order_id, symbol,
                                                                          'Lmt' if random.randint(1, 100) % 2 == 0 else 'Mkt',side=my_position, price=my_price, qty=my_qty)
                                                        set_event(e)
                                                        send_msg(client_packet)
                                                        wait_for_an_event(e)
                                                        orders.append(get_response(q))

                                                        new_order = orders[-1]
                                                        if new_order['Status'] != "Order Reject":
                                                            new_order['Total Filled'] = 0
                                                            new_order['Profit_Loss'] = 0.0
                                                            orders_list.append(new_order)
                                                            orders_record.update({market_period: orders_list})
                                                        else:
                                                            pass


                                print(orders)
                                print(orders_record)

            elif market_status=="Pending Closing":
                 print("=================Pending Closing=================")

                 if bool(orders_record) == False:
                     pass

                 else:
                     today_orders=orders_record[market_period]

                     for order in today_orders:

                        if order['Total Filled'] == int(order['Qty']):
                            continue

                        print("************Close Today's Orders Position************")

                        while True:
                                client_packet = Packet()
                                client_order_id = order['OrderIndex']+'Close'
                                enter_a_new_order(client_packet, client_order_id, order['Symbol'],
                                                  'Mkt', 'Sell' if order['Side']=='Buy' else 'Buy', 0, qty=(int(order['Qty'])-order['Total Filled']))

                                set_event(e)
                                send_msg(client_packet)
                                wait_for_an_event(e)
                                orders.append(get_response(q))
                                order_place=orders[-1]

                                if order_place['Status'] != 'Order Reject':

                                    order['Total Filled'] += int(order_place['Qty'])
                                    price_id+=1
                                    qty_id+=1
                                    price_ID ='Close_Price'+ str(price_id)
                                    qty_ID ='Close_Qty'+ str(qty_id)
                                    order[price_ID] = order_place['Price']
                                    order[qty_ID] = order_place['Qty']

                                    if order['Side'] == 'Buy':
                                        order['Profit_Loss'] = (order_place['Price'] - float(order['Price'])) * int(order_place['Qty'])
                                    else:
                                        order['Profit_Loss'] = -(order_place['Price'] - float(order['Price'])) * int(order_place['Qty'])

                                if order['Total Filled'] >= int(order['Qty']):
                                    break

                 print("***********Orders Update************")
                 print(orders_record)

            elif market_status == "Market Closed":
                print("=================Market Close=================")

                if market_period not in market_close_dict.keys():
                        market_close_dict.update({market_period:[]})

                elif (market_period in market_close_dict.keys()) and (order_book != market_close_dict[market_period]):
                        market_close_dict.update({market_period: order_book})

                elif (market_period in market_close_dict.keys()) and (order_book == market_close_dict[market_period]):
                        stocks_data = {}
                        signal_dict.update({market_period: stocks_data})
                        my_orderbook= market_close_dict[market_period]

                        for stock in stock_list:
                            data_list = [None] * 4
                            stocks_data.update({stock: data_list})

                        for key in stocks_data:
                            filled_price=[]

                            for order in my_orderbook:

                                if order['Symbol']==key:

                                    filled_price.append(order['Price'])

                                    if order['Status'] == 'Open Trade':
                                        stocks_data[key][0]=order['Price']

                                    elif order['Status'] == 'Close Trade':
                                         stocks_data[key][3] = order['Price']

                                    #elif order['Status'] == 'Filled':
                                    #filled_price.append(order['Price'])

                            stocks_data[key][1] = max(filled_price)
                            stocks_data[key][2] = min(filled_price)
                            signal_dict.update({market_period: stocks_data})

                        for date in signal_dict.keys():
                            data=signal_dict[date]
                            for symbol in data.keys():
                                value=data[symbol]
                                for i in range(len(value)):
                                    if value[i] == None:
                                        value[i] = (value[1]+value[2])/2

                        print("Get Open High Low Close......")
                        print(signal_dict)

                        if len(signal_dict)>=13 : #change to 14
                           all_dates=list(signal_dict.keys())
                           dates_list=all_dates[-13:] #change to 14
                           last_date=all_dates[-1]
                           if last_date not in trades_signal.keys():
                              signals = {}
                              trades_signal.update({last_date:signals})

                              #caculate r breaker
                              r_breaker_data=signal_dict[last_date]
                              for symbol in r_breaker_data:
                                    price=list(r_breaker_data[symbol])
                                    signals[symbol]={}
                                    signals[symbol]['Open'] = price[0]
                                    signals[symbol]['High'] = price[1]
                                    signals[symbol]['Low'] = price[2]
                                    signals[symbol]['Close'] = price[3]
                                    signals[symbol]['PivotPoint'] = (price[1] + price[2] + price[3]) / 3
                                    signals[symbol]['SellCheck'] = price[1] + 0.33 * (price[3] - price[2])
                                    signals[symbol]['RevSell'] = (1 + 0.07) / 2 * (price[1] + price[2]) - 0.07 * price[2]
                                    signals[symbol]['RevBuy'] = (1 + 0.07) / 2 * (price[1] + price[2]) - 0.07 * price[1]
                                    signals[symbol]['BuyCheck'] = price[2] - 0.33 * (price[1] - price[3])
                                    signals[symbol]['TrendBuy'] = signals[symbol]['SellCheck'] + 0.24 * (signals[symbol]['SellCheck'] - signals[symbol]['BuyCheck'])
                                    signals[symbol]['TrendSell'] = signals[symbol]['BuyCheck'] - 0.24 * (signals[symbol]['SellCheck'] - signals[symbol]['BuyCheck'])

                              #calculate atr
                              for date in dates_list:
                                    atr_data=signal_dict[date]
                                    for key in atr_data:
                                        sum=0
                                        price=atr_data[key]
                                        value1=price[1]-price[2]
                                        value2=abs(price[1]-price[3])
                                        value3=abs(price[2]-price[3])
                                        max_value = max(value1,value2,value3)
                                        sum+=max_value
                                        signals[key]['ATR']=sum/13   #change to 14

                        print("Calculate Trading Signals......")
                        print(trades_signal)
            time.sleep(2)


        bTradeComplete = True

        print("***********Print Trading Results************")
        #print(orders)
        print(orders_record)


    except(OSError, Exception):
        # except(OSError):
        q.put(PacketTypes.CONNECTION_NONE.value, Exception('join_trading_network'))
        client_socket.close()
        sys.exit(0)

    return orders_record


@app.route('/')
def index():
    stocks = pd.read_csv(location_of_stocks)
    stocks = stocks.transpose()
    list_of_stocks = [stocks[i] for i in stocks]
    return render_template("index.html", stock_list=list_of_stocks)

@app.route('/build_model')
def build_model():
    #build_trading_model()
    select_st = "Select * from Open_Position;"
    result_set = execute_sql_statement(select_st, engine)
    result_df = pd.DataFrame(result_set.fetchall())
    result_df.columns = result_set.keys()
    result_df = result_df.transpose()
    list_of_stocks = [result_df[i] for i in result_df]
    return render_template("build_model.html", stock_list=list_of_stocks)

@app.route('/back_test')
def model_back_testing():
    #backtesting(metadata, engine)
    select_st = "Select * from Trades;"
    result_set = execute_sql_statement(select_st, engine)
    result_df = pd.DataFrame(result_set.fetchall())
    result_df.columns = result_set.keys()
    result_df['Profit_Loss'] = result_df['Profit_Loss'].map('${:,.2f}'.format)
    result_df = result_df.transpose()
    list_of_stocks = [result_df[i] for i in result_df]
    return render_template("back_testing.html", stock_list=list_of_stocks)

@app.route('/trade_analysis')
def trade_analysis():
    select_st = "SELECT Symbol, printf(\"US$%.2f\", sum(Profit_Loss)) AS Profit, count(Profit_Loss) AS Total_Trades, \
                sum(CASE WHEN Profit_Loss > 0 THEN 1 ELSE 0 END) AS Profit_Trades, \
                sum(CASE WHEN Profit_Loss <=0 THEN 1 ELSE 0 END) AS Loss_Trades FROM Trades\
                Group BY Symbol;"
    result_set = execute_sql_statement(select_st, engine)
    result_df = pd.DataFrame(result_set.fetchall())
    result_df.columns = result_set.keys()
    print(result_df.to_string(index=False))
    result_df = result_df.transpose()
    trade_results = [result_df[i] for i in result_df]
    return render_template("trade_analysis.html", trade_list=trade_results)

@app.route('/real_trade_analysis')
def real_trade_analysis():

    trade_result={}

    for symbol in symbol_list:
        trade_result.update({symbol:{}})
        trade_result[symbol]['Profit']=0.0
        trade_result[symbol]['Total_Trades']=0
        trade_result[symbol]['Profit_Trades']=0
        trade_result[symbol]['Loss_Trades']=0
        for key, order_list in orders_record.items():
            for order in order_list:
                if order['Symbol']==symbol:
                    trade_result[symbol]['Profit'] += order['Profit_Loss']
                    trade_result[symbol]['Total_Trades']+=1

                    if order['Profit_Loss']>=0:
                        trade_result[symbol]['Profit_Trades'] += 1
                    else:
                        trade_result[symbol]['Loss_Trades'] += 1

    result_df=pd.DataFrame.from_dict(trade_result, orient='index')
    result_df=result_df.rename_axis('Symbol').reset_index()
    result_df['Profit'] = result_df['Profit'].map('${:,.2f}'.format)
    print(result_df)
    result_df = result_df.transpose()
    trade_results = [result_df[i] for i in result_df]
    return render_template("real_trade_analysis.html", trade_list=trade_results)

@app.route('/start_trading')
def start_trading():
    global bClientThreadStarted
    global client_thread

    if bClientThreadStarted == False:
        client_thread.start()
        bClientThreadStarted = True

    while bTradeComplete == False:
        pass

    return render_template("start_trading.html", trading_results=orders)


@app.route('/trading_results', methods=['POST', 'GET'])
def trading_result():
    if request.method == 'POST':
        form_input = request.form
        client_packet = Packet()
        client_msg = enter_a_new_order(client_packet, 1, form_input['Symbol'], form_input['Side'], form_input['Price'],
                                       form_input['Quantity'])
        send_msg(client_msg)
        data = get_response(q)
        return render_template("trading_results.html", trading_results=data)

@app.route('/client_down')
def client_down():
    client_packet = Packet()
    msg_data = {}
    try:
        send_msg(quit_connection(client_packet))
        msg_type, msg_data = q.get()
        q.task_done()
        print(msg_data)
        return render_template("client_down.html", server_response=msg_data)
    except(OSError, Exception):
        print(msg_data)
        return render_template("client_down.html", server_response=msg_data)


if __name__ == "__main__":

    #build_trading_model()
    #backtesting(metadata, engine)
    #app.run()

    try:
        order_table = []
        # client_thread = threading.Thread(target=send, args=(q,))
        client_thread = threading.Thread(target=join_trading_network, args=(q, e))
        bClientThreadStarted = False

        app.run()

        if client_thread.is_alive() is True:
            client_thread.join()

    except (KeyError, KeyboardInterrupt, SystemExit, RuntimeError, Exception):
        client_socket.close()
        sys.exit(0)
