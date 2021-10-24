import websocket
import pandas as pd
import numpy as np
from datetime import datetime
import ast
import sqlalchemy
import pymysql


# Connect to MySQL Server
engine = sqlalchemy.create_engine('mysql+pymysql://root:root@localhost/kryptotrading?charset=utf8mb4')
try:
    conn = engine.connect()
    stmt = sqlalchemy.text("drop table etheur")
    conn.execute(stmt)
except Exception as e:
    print(e)

# Connect to WebSocket API and subscribe to trade feed for ETH/EUR
ws = websocket.create_connection("wss://ws.kraken.com/")
ws.send('{"event":"subscribe", "subscription":{"name":"trade"}, "pair":["ETH/EUR"]}')

# Infinite loop waiting for WebSocket data
try:
    while True:
        message = ws.recv()
        if(message[0] == "["):
            obj = ast.literal_eval(message)
            print(obj)
            data = np.array([
                [
                    obj[3],
                    datetime.fromtimestamp(float(obj[1][0][2])),
                    float(obj[1][0][0])
                ]
            ])
            df = pd.DataFrame(data, columns=["symbol", "time", "price"])
            df.to_sql('etheur', engine, if_exists='append', index=False)
            print(df)
        else:
            if(ast.literal_eval(message)["event"] == "heartbeat"):
                print("Heartbeat detected")
except KeyboardInterrupt as e:
    print(e)