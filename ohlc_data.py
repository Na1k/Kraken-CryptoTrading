# Import WebSocket client library (and others)
from sqlalchemy import engine
import websocket
import _thread
import time
import sqlalchemy
from datetime import datetime
import pandas as pd
import ast

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

def ws_open(ws):
    ws.send('{"event":"subscribe", "subscription":{"name":"ohlc", "interval":1}, "pair":["ETH/EUR"]}')

def ws_thread(*args):
    ws = websocket.WebSocketApp("wss://ws.kraken.com/", on_open = ws_open, on_message = ws_message)
    ws.run_forever()

# Start a new thread for the WebSocket interface
_thread.start_new_thread(ws_thread, ())

# Continue other (non WebSocket) tasks in the main thread
while True:
    time.sleep(10)
    df = pd.read_sql("select * from etheur_ohlc;", engine)
    df = df.drop_duplicates(subset=["time_end"], keep='last')
    df.to_sql('etheur_ohlc', engine, if_exists='replace', index=False)
    print("Cleaned Database")