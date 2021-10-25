import sqlalchemy
import pandas as pd
import matplotlib.pyplot as plt
import pymysql
from datetime import datetime
import scipy.optimize

def gerade(_x,a,b):
    return a+b*_x


#engstr = 'mysql+pymysql://root:root@localhost/kryptotrading?charset=utf8mb4'
engstr = 'sqlite:///trading.db'

engine = sqlalchemy.create_engine(engstr)
df = pd.read_sql('SELECT * from etheur', engine)
df["time"] = df["time"].map(lambda time: datetime.fromisoformat(time))
df['150 SMA'] = df['price'].rolling(150).mean()
plt = df.plot(x="time",y=["price","150 SMA"],figsize=(15, 12)).get_figure()
plt.savefig('ETHEUR-chart.png')

df['strat'] = df['price'] - df['150 SMA']
df.loc[df['strat'] >= 0, 'strat1'] = "long"
df.loc[df['strat'] < 0, 'strat1'] = "short"

tmp = df[['150 SMA', 'time']].copy()
tmp = tmp.dropna()

arr = []
tmp['time'] = tmp['time'].apply(lambda x: x.value)
print(tmp)

res = tmp.diff()
res = res.dropna()
print(res)
res["time"] = res['time'].apply(lambda x: datetime.fromtimestamp(x))

plt = res.plot(x="time",y="150 SMA",figsize=(15, 12)).get_figure()

plt.savefig("deriv.png")