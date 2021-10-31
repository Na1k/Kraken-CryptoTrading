import websocket
import pandas as pd
import numpy as np
from datetime import datetime
import ast
import sqlalchemy
import pymysql

def droptable():
    try:
        conn = engine.connect()
        stmt = sqlalchemy.text("drop table etheur")
        conn.execute(stmt)
    except Exception as e:
        print(e)

def websocket_prices():
    # Connect to WebSocket API and subscribe to trade feed for ETH/EUR
    ws = websocket.create_connection("wss://ws.kraken.com/")
    ws.send('{"event":"subscribe", "subscription":{"name":"trade"}, "pair":["ETH/EUR"]}')
    # Infinite loop waiting for WebSocket data
    try:
        while True:
            message = ws.recv()
            if(message[0] == "["):
                obj = ast.literal_eval(message)
                #print(obj)
                df = pd.DataFrame(columns=["symbol", "time", "price"])
                for i in range(0,len(obj[1])):
                    data = np.array([
                        [
                            obj[3],
                            datetime.fromtimestamp(float(obj[1][i][2])),
                            float(obj[1][i][0])
                        ]
                    ])
                    tmp = pd.DataFrame(data, columns=["symbol", "time", "price"])
                    df = df.append(tmp)
                df = df.iloc[::-1]
                df.to_sql('etheur', engine, if_exists='append', index=False)
                print(df)
            """
            else:
                if(ast.literal_eval(message)["event"] == "heartbeat"):
                    print("Heartbeat detected")
            """
    except KeyboardInterrupt as e:
        print(e)


engstr = 'mysql+pymysql://root:root@localhost/kryptotrading?charset=utf8mb4'
#engstr = 'sqlite:///trading.db'

# Connect to MySQL Server
engine = sqlalchemy.create_engine(engstr)
#droptable()
