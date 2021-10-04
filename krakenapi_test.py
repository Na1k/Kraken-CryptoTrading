import krakenex

kraken = krakenex.API()
kraken.load_key('kraken.key')
#print(kraken.query_public('Ticker', {'pair':'ETHEUR'}))
#a - ask array (price, whole lot volume, lot volume)
#b - bid array (price, whole lot volume, lot volume)
#c - last trade closed (price, lot volume)
#v - Volume (today, last 24h)
#p - volume weighted average price array (today, last 24h)
#t - number of trades array (today, last 24h)
#l - low (today, last 24h)
#h - high (today, last 24h)
#o - today opening price

#kraken.query_private('Balance')
history = kraken.query_public('OHLC',{'pair':'ETHEUR', 'interval':1440, 'ascending':True})
print(history['result[XETHEUR]'])