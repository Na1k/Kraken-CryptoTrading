import sqlalchemy
import pandas as pd
import matplotlib.pyplot as plt
import time
from datetime import datetime
import numpy as np



def get_dataframe_from_sql(engstr, sql_stmt):
    engine = sqlalchemy.create_engine(engstr)
    df = pd.read_sql(sql_stmt, engine)    #columns: price, time
    df = df.sort_values(by=["time"])
    df = df.reset_index()
    return df.copy()

def calc_SMA(df, ticks):
    #df["time"] = df["time"].map(lambda time: datetime.fromisoformat(time))
    return df['price'].rolling(window = ticks).mean()

def plot_dataframe(df,x,y,name):
    fig = df.plot(x=x, y=y, figsize=(15, 12)).get_figure()
    plt.savefig("figures\\"+name)
    plt.close(fig)

def calc_rise(df):
    df = df.diff()
    return df

def long_short(df):
    df['strat'] = df['price'] - df['SMA']
    df.loc[df['strat'] >= 0, 'strat1'] = "long"
    df.loc[df['strat'] < 0, 'strat1'] = "short"
    return df['strat1']

def decision(df, rem):
    if((df['losho'][df.index[-1]] == "long") & (not rem)):
        print("KAUFEN", datetime.now())
        return True
    elif(df['losho'][df.index[-1]] == "short"):
        return False
    else:
        return True

engstr = 'mysql+pymysql://root:root@localhost/kryptotrading?charset=utf8mb4'
#engstr = 'sqlite:///trading.db'
remember = True

df = get_dataframe_from_sql(engstr, 'SELECT * FROM etheur;')

df['SMA'] = calc_SMA(df, 700)

plot_dataframe(df,"time",["price", "SMA"],'ETHEUR-init.png')

df["losho"] = long_short(df.copy())

df['rise'] = calc_rise(df['SMA'].copy())
fig = df.plot( y="rise",use_index=True, figsize=(15, 12)).get_figure()
fig.savefig("figures\\deriv.png")
plt.close(fig)

while True:
    df = get_dataframe_from_sql(engstr, "select * from etheur order by time desc limit 750;")
    
    df['SMA'] = calc_SMA(df, 700)
    plot_dataframe(df,"time",["price", "SMA"],'Live-etheur.png')
    df["losho"] = long_short(df.copy())
    df['rise'] = calc_rise(df['SMA'].copy())
    remember = decision(df.copy(), remember)
    time.sleep(5)

"""
tmp = df[['SMA', 'time']].copy()
tmp = tmp.dropna()

arr = []
tmp['time'] = tmp['time'].values.astype('timedelta64[ns]')
tmp['time'] = tmp['time'].dt.total_seconds()

res = tmp.diff()
print(res)
res = res.dropna()
res["rise"] = res['SMA'].div(res['time'].values)
res.replace([np.inf, -np.inf], np.nan, inplace=True)
res = res.dropna()
print(res)


plt = res.reset_index().plot(kind='scatter', y="rise",x="index", figsize=(15, 12)).get_figure()

plt.savefig("deriv.png")
"""