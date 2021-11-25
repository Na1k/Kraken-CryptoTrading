import pandas as pd
import numpy as np
from datetime import datetime
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


def strat(df, open_position):
    if(not(open_position)):
        if((df["RSI"].values < 30) & (df["percent_b"].values < 0)):
            print("KAUFEN", datetime.now(), "Preis", df["close"].values)
            df2 = df[['close', 'time_end']].copy()
            df2["desc"] = ["b"]
            df2.to_sql('strat', engine, if_exists='append', index=False)
            return True
    elif(open_position):
        if((df["RSI"].values > 70) & (df["percent_b"].values > 1)):
            print("VERKAUFEN", datetime.now(), "Preis", df["close"].values)
            df2 = df[['close', 'time_end']].copy()
            df2["desc"] = ["b"]
            df2.to_sql('strat', engine, if_exists='append', index=False)
            return False
    return open_position


engstr = 'mysql+pymysql://root:root@localhost/kryptotrading?charset=utf8mb4'
engine = sqlalchemy.create_engine(engstr)
open_pos = False
while True:
    try:
        df = get_dataframe_from_sql(engine, 'SELECT * FROM etheur_ohlc;')
        df = df.drop_duplicates(subset=["time_end"], keep='last').sort_values(
            by="time_end", ascending=True).reset_index().drop(columns="index")
        df["RSI"] = get_RSI(df, 14)
        df["MACD"], df["MACD_sig"], df["MACD_hist"] = get_MACD(df, 12, 26, 9)
        df["BBupper"], df["BBmiddle"], df["BBlower"] = get_BBANDS(
            df, 12, 2, 2.4, MA_Type.T3)
        df["percent_b"] = get_percentB(df)
        printOhlcImage(df, "ohlc.png")
        open_pos = strat(df.iloc[-1:], open_pos)
    except (Exception, KeyboardInterrupt) as e:
        print(e)
    time.sleep(.5)
