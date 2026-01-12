#
# Simple Tick Data Trader
# (SMA Online Algorithm)
#
import zmq
import datetime
import pandas as pd

context = zmq.Context()
socket = context.socket(zmq.SUB)
socket.connect('tcp://127.0.0.1:5555')

socket.setsockopt_string(zmq.SUBSCRIBE, 'AAPL')

ticks = pd.DataFrame()

SMA1 = 3
SMA2 = 6

position = 0
min_bars = SMA2 + 1

while True:
    msg = socket.recv_string()
    t = datetime.datetime.now()
    print(str(t) + ' | ' + msg + ' | position = %d' % position)
    symbol, price = msg.split()
    ticks = ticks.append(
            pd.DataFrame({'symbol': symbol, 'price': float(price)},
                index=[t,]))
    data = ticks.resample('5s', label='right').last().ffill()
    data['SMA1'] = data['price'].rolling(SMA1).mean()
    data['SMA2'] = data['price'].rolling(SMA2).mean()
    if len(data) > min_bars:
        min_bars += 1
        # print(data.tail())
        if position in [0, -1]:
            if data['SMA1'].iloc[-2] > data['SMA2'].iloc[-2]:
                print('*** GOING LONG ***')
                # place trading code here
                position = 1
        elif position in [0, 1]:
            if data['SMA1'].iloc[-2] < data['SMA2'].iloc[-2]:
                print('*** GOING SHORT ***')
                # place trading code here
                position = -1

