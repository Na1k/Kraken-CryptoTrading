import sqlalchemy
import pandas as pd
import matplotlib.pyplot as plt
import time
from datetime import datetime
import numpy as np
import talib



def get_dataframe_from_sql(engstr, sql_stmt):
    engine = sqlalchemy.create_engine(engstr)
    df = pd.read_sql(sql_stmt, engine)    #columns: price, time
    df = df.sort_values(by=["time"])
    df = df.reset_index()
    return df.copy()

def calc_SMA(df, ticks):
    #df["time"] = df["time"].map(lambda time: datetime.fromisoformat(time))
    return df['price'].rolling(window = ticks).mean()

def calc_EMA(df, ticks, smoothing=2):
    df["EMA"] = calc_SMA(df.copy(), ticks)

    return

def plot_dataframe(df,x,y,name):
    fig = df.plot(x=x, y=y, figsize=(15, 12)).get_figure()
    plt.savefig("figures\\"+name)
    plt.close(fig)

def calc_rise(df):
    df = df.diff()
    return df

def long_short(df, col1, col2):
    df['strat'] = df[col1] - df[col2]
    df.loc[df['strat'] >= 0, 'strat1'] = "long"
    df.loc[df['strat'] < 0, 'strat1'] = "short"
    return df['strat1']

def decision(df, rem):
    if((df['losho1'][df.index[-1]] == "long") & (df['losho2'][df.index[-1]] == "long") & (df['losho3'][df.index[-1]] == "long") & (not rem)):
        print("KAUFEN", datetime.now(), "Preis", df["price"][df.index[-1]])
        return True
    elif((df['losho1'][df.index[-1]] == "short") | ((df['losho2'][df.index[-1]] == "short") & (df['losho3'][df.index[-1]] == "short"))):
        return False
    else:
        return rem

engstr = 'mysql+pymysql://root:root@localhost/kryptotrading?charset=utf8mb4'
#engstr = 'sqlite:///trading.db'
remember = True

df = get_dataframe_from_sql(engstr, 'SELECT * FROM etheur;')
df = df.drop_duplicates(keep='last')
df['SMA'] = calc_SMA(df, 700)
df['sSMA'] = calc_SMA(df,100)

plot_dataframe(df,"time",["price", "SMA", "sSMA"],'ETHEUR-init.png')

df['rise'] = calc_rise(df['SMA'].copy())
fig = df.plot( y="rise",use_index=True, figsize=(15, 12)).get_figure()
fig.savefig("figures\\deriv.png")
plt.close(fig)

while True:
    df = get_dataframe_from_sql(engstr, "select * from etheur order by time desc limit 850;")
    df.drop_duplicates(keep='last')
    df['SMA'] = calc_SMA(df, 800)
    df['sSMA'] = calc_SMA(df,200)
    plot_dataframe(df,"time",["price", "SMA", "sSMA"],'Live-etheur.png')
    df["losho1"] = long_short(df.copy(), "price", "SMA")
    df["losho2"] = long_short(df.copy(), "price", "sSMA")
    df["losho3"] = long_short(df.copy(), "sSMA", "SMA")
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