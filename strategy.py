import pandas as pd
import numpy as np
from datetime import datetime
import krakenex
import sqlalchemy
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import time
import talib
from talib import MA_Type


def get_dataframe_from_sql(eng, sql_stmt):
    df = pd.read_sql(sql_stmt, eng)
    return df.copy()


def printOhlcImage(df, name):
    fig = make_subplots(rows=4, cols=1, shared_xaxes=True,
                        vertical_spacing=0.01,
                        row_heights=[0.6, 0.15, 0.15, 0.1])
    fig.add_trace(go.Candlestick(x=df['time_end'],
                                 open=df['open'],
                                 high=df['high'],
                                 low=df['low'],
                                 close=df['close']), row=1, col=1)
    fig.add_trace(go.Scatter(x=df["time_end"],
                             y=df["BBupper"]
                             ), row=1, col=1)
    fig.add_trace(go.Scatter(x=df["time_end"],
                             y=df["BBmiddle"]
                             ), row=1, col=1)
    fig.add_trace(go.Scatter(x=df["time_end"],
                             y=df["BBlower"]
                             ), row=1, col=1)
    fig.add_trace(go.Scatter(x=df["time_end"],
                             y=df["percent_b"],
                             line=dict(color='black', width=1)
                             ), row=2, col=1)
    fig.add_trace(go.Bar(x=df["time_end"],
                         y=df["MACD_hist"]
                         ), row=4, col=1)
    fig.add_trace(go.Scatter(x=df["time_end"],
                             y=df["MACD"],
                             line=dict(color='black', width=2)
                             ), row=4, col=1)
    fig.add_trace(go.Scatter(x=df["time_end"],
                             y=df["MACD_sig"],
                             line=dict(color='blue', width=1)
                             ), row=4, col=1)
    fig.add_trace(go.Scatter(x=df["time_end"],
                             y=df["RSI"],
                             line=dict(color='red', width=1)
                             ), row=3, col=1)
    fig.update_layout(height=900, width=1200,
                      showlegend=False,
                      xaxis_rangeslider_visible=False)
    fig.update_yaxes(title_text="Price", row=1, col=1)
    fig.update_yaxes(title_text="BBP", row=2, col=1)
    fig.update_yaxes(title_text="RSI", row=3, col=1)
    fig.write_image("figures\\"+name)
    # fig.show() #output in browser

def get_RSI(df, per):
    return talib.RSI(df["close"], timeperiod=per)

def get_MACD(df, fp, slowp, sigp):
    macd, macdsignal, macdhist = talib.MACD(
        df["close"], fastperiod=fp, slowperiod=slowp, signalperiod=sigp)
    return macd, macdsignal, macdhist

def get_BBANDS(df, tp, nbdup, nbddn, mt):
    upperband, middleband, lowerband = talib.BBANDS(
        df["close"], timeperiod=tp, nbdevup=nbdup, nbdevdn=nbddn, matype=mt)
    return upperband, middleband, lowerband

def get_percentB(df):
    return ((df["close"]-df["BBlower"])/(df["BBupper"] - df["BBlower"]))

def buy(df, info):
    try:
        balance = kraken.query_private("Balance")
        balance = float(balance['result']['ZEUR'])
        ETHval = df['close'].values[0]
        info['volume'] = 0.95*(balance/ETHval)
        kraken.query_private('AddOrder',
                            {'pair': 'ETHEUR',
                            'type': 'buy',
                            'ordertype': 'market',
                            'volume': info['volume']
                            })
        info['stoploss'] = df["close"].values[0] - 75
        info['open_position'] = True
        print("KAUFEN", datetime.now(), "Preis", df["close"].values[0], flush=True)
    except (Exception) as e:
        info['open_position'] = False
        print(e, flush=True)
    print(info, flush=True)
    return info

def sell(df, info):
    try:
        kraken.query_private('AddOrder',
                            {'pair': 'ETHEUR',
                            'type': 'sell',
                            'ordertype': 'market',
                            'volume': info['volume']
                            })
        info['open_position'] = False
        info['stoploss'] = 0
        info['volume'] = 0
        print("VERKAUFEN", datetime.now(), "Preis", df["close"].values[0], flush=True)
    except (Exception) as e:
        print(e, flush=True)
    print(info, flush=True)
    return info

def asc_stoploss(df, info):
    stoploss_now = df["close"].values[0] - 50
    if(info['stoploss'] < stoploss_now):
        info['stoploss'] = stoploss_now
    return info

def strat(df, info):
    #Check if database is up-to-date
    time_df = df["time_end"].values[0]
    time_now = np.datetime64(datetime.now())
    if(not(time_now < time_df) or (not(time_now > (time_df - np.timedelta64(5,"m"))))):
        time.sleep(5)
        return info
    
    if(not(info['open_position'])):     #Buy Descision
        if((df["RSI"].values < 30) & (df["percent_b"].values < 0)):
            info = buy(df, info)
            df2 = df[['close', 'time_end']].copy()
            df2["desc"] = ["b"]
            df2.to_sql('strat', engine, if_exists='append', index=False)
            return info
    elif(info['open_position']):
        if(df['close'].values[0] < info['stoploss']):   #Stop-Loss Descision
            info = sell(df, info)
            df2 = df[['close', 'time_end']].copy()
            df2["desc"] = ["sl"]
            df2.to_sql('strat', engine, if_exists='append', index=False)
            time.sleep(900)
            return info
        else:
            info = asc_stoploss(df, info)       #check for ascending Stop-Loss if Stop-Loss didn't trigger
        
        if((df["RSI"].values > 70) & (df["percent_b"].values > 1)):     #Selling Descision
            info = sell(df, info)
            df2 = df[['close', 'time_end']].copy()
            df2["desc"] = ["s"]
            df2.to_sql('strat', engine, if_exists='append', index=False)
            return info
    return info

kraken = krakenex.API()
kraken.load_key('./kraken.key')
engstr = 'mysql+pymysql://root:root@localhost/kryptotrading?charset=utf8mb4'

engine = sqlalchemy.create_engine(engstr)
trade = {'open_position':False,
         'stoploss':0,
         'volume':0}

while True:
    try:
        df = get_dataframe_from_sql(engine, 'SELECT * FROM etheur_ohlc_5;')
        df = df.drop_duplicates(subset=["time_end"], keep='last').sort_values(
            by="time_end", ascending=True).reset_index().drop(columns="index")
        df["RSI"] = get_RSI(df, 14)
        df["MACD"], df["MACD_sig"], df["MACD_hist"] = get_MACD(df, 12, 26, 9)
        df["BBupper"], df["BBmiddle"], df["BBlower"] = get_BBANDS(
            df, 12, 2, 2.4, MA_Type.T3)
        df["percent_b"] = get_percentB(df)
        printOhlcImage(df, "ohlc.png")
        trade = strat(df.iloc[-1:], trade)
    except (Exception, KeyboardInterrupt) as e:
        print(e, flush=True)
    time.sleep(1.)
