#
# Simple Tick Data Collector 
#
import zmq
import datetime
import pandas as pd

context = zmq.Context()
socket = context.socket(zmq.SUB)
socket.connect('tcp://127.0.0.1:5555')

socket.setsockopt_string(zmq.SUBSCRIBE, 'AAPL')

ticks = pd.DataFrame()

while True:
    msg = socket.recv_string()
    t = datetime.datetime.now()
    print(str(t) + ' | ' + msg)
    symbol, price = msg.split()
    ticks = ticks.append(
            pd.DataFrame({'symbol': symbol, 'price': float(price)},
                index=[t,]))
    data = ticks.resample('5s', label='right').last().ffill()

