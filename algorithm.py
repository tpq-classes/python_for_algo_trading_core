#
# DNN Strategy for BTC/USD
# with FXCM
#
import time
import tensorflow as tf
import keras
import fxcmpy
import pickle
import numpy as np
import pandas as pd
import datetime as dt

algorithm = keras.models.load_model('algorithm.keras')
params = pickle.load(open('params.pkl', 'rb'))

ticks = 0
position = 0
min_length = params['lags']
symbol = 'BTC/USD'
features = ['r', 'sma', 'mom', 'vol']
sel = ['tradeId', 'amountK', 'currency',
       'grossPL', 'isBuy']


def add_lags(data, lags=params['lags']):
    cols = list()
    for f in features[:]:
        for lag in range(1, lags + 1):
            col = f'{f}_lag_{lag}'
            data[col] = data[f].shift(lag)
            cols.append(col)
    return data, cols


def prepare_features(resam):
    data = pd.DataFrame(resam['Mid'])
    data.columns = [symbol]
    data['r'] = np.log(data / data.shift(1))
    data['sma'] = data[symbol].rolling(3).mean()
    data['mom'] = data['r'].rolling(3).mean() 
    data['vol'] = data['r'].rolling(3).std()
    data, cols = add_lags(data)
    return data, cols


def btc_dnn(data, df):
    global ticks
    global position
    global min_length
    ticks += 1
    print(ticks, end=' ')
    resam = df.resample('5s', label='right').last().ffill()
    resam['Mid'] = (resam['Bid'] + resam['Ask']) / 2
    data, cols = prepare_features(resam.iloc[:-1])
    data_ = (data - params['mu']) / params['mu']
    if len(data) > min_length:
        min_length += 1
        signal = np.where(algorithm.predict(
            np.atleast_2d(data_[cols].iloc[-1])) > 0.5, 1, -1)[0, 0]
        print(f'\n*** SIGNAL = {signal}')
        if position in [0, 1] and signal == -1:
            print('\n*** GOING SHORT***')
            order = api.create_market_sell_order('BTC/USD', (1 + position) * 500)
            print(api.get_open_positions()[sel])
            position = -1
        elif position in [0, -1] and signal == 1:
            print('\n*** GOING LONG ***')
            order = api.create_market_buy_order('BTC/USD', (1 - position) * 500)
            print(api.get_open_positions()[sel])
            position = 1
                        
if __name__ is '__main__':
    api = fxcmpy.fxcmpy(config_file='pyalgo.cfg')
    api.subscribe_market_data('BTC/USD', (btc_dnn,))
