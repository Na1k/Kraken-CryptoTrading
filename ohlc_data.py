import websocket
import _thread
import time
import sqlalchemy
from datetime import datetime
import pandas as pd
import ast
import krakenex
from datetime import datetime, timedelta
import numpy as np

engstr = 'mysql+pymysql://root:root@localhost/kryptotrading?charset=utf8mb4'
#engstr = 'sqlite:///trading.db'
engine = sqlalchemy.create_engine(engstr)

def sqlExec(str):
    try:
        conn = engine.connect()
        stmt = sqlalchemy.text(str)
        conn.execute(stmt)
    except Exception as e:
        print(e)

# Define WebSocket callback functions
def ws_message(ws, message):
    if(message[0] == "["):
        obj = ast.literal_eval(message)
        data = [obj[1][1:-1]]    
        df = pd.DataFrame(data, columns=["time_end", "open", "high", "low", "close", "vwap" ,"volume"])
        df["time_end"] = datetime.fromtimestamp(float(df["time_end"]))
        df = df.drop(columns=["vwap"])
        df.to_sql('etheur_ohlc', engine, if_exists='append', index=False)
        print(df)

def ws_open(ws):
    ws.send('{"event":"subscribe", "subscription":{"name":"ohlc", "interval":5}, "pair":["ETH/EUR"]}')

def ws_thread(*args):
    ws = websocket.WebSocketApp("wss://ws.kraken.com/", on_open = ws_open, on_message = ws_message)
    ws.run_forever()

def load_old(past_minutes):
    sqlExec("DROP table etheur_ohlc;")
    kraken = krakenex.API()
    last_hour = datetime.now() - timedelta(minutes = past_minutes)
    history = kraken.query_public(
        'OHLC', {'pair': 'ETHEUR', 'since': last_hour.timestamp(), 'interval': 5, 'ascending': True}) #
    
    for el in history['result']['XETHZEUR']:
        data = np.array([
            datetime.fromtimestamp(el[0]),
            float(el[1]),
            float(el[2]),
            float(el[3]),
            float(el[4]),
            float(el[6])
        ])
        df = pd.DataFrame([data], columns=["time_end", "open", "high", "low", "close", "volume"])
        df.to_sql('etheur_ohlc', engine, if_exists='append', index=False)

load_old(300)
# Start a new thread for the WebSocket interface
_thread.start_new_thread(ws_thread, ())

# Continue other (non WebSocket) tasks in the main thread
while True:
    time.sleep(300)
    df = pd.read_sql("select * from etheur_ohlc;", engine)
    df = df.drop_duplicates(subset=["time_end"], keep='last')
    df.to_sql('etheur_ohlc', engine, if_exists='replace', index=False)
    print("Cleaned Database")