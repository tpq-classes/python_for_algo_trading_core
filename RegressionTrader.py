#
# Python Script
# with Linear Regression Trading Class
# for Oanda v20
#
# Python for Algorithmic Trading
# (c) Dr. Yves J. Hilpisch
# The Python Quants GmbH
#
import tpqoa
import numpy as np
import pandas as pd


class RegressionTrader(tpqoa.tpqoa):
    def __init__(self, conf_file, instrument, bar_length, reg_length, units,
                 *args, **kwargs):
        super(RegressionTrader, self).__init__(conf_file)
        self.position = 0
        self.instrument = instrument
        self.reg_length = reg_length
        self.bar_length = bar_length
        self.units = units
        self.raw_data = pd.DataFrame()
        self.min_length = self.reg_length + 1

    def on_success(self, time, bid, ask):
        ''' Takes actions when new tick data arrives. '''
        print(self.ticks, end=' ')
        self.raw_data = self.raw_data.append(pd.DataFrame(
            {'bid': bid, 'ask': ask}, index=[pd.Timestamp(time)]))
        self.data = self.raw_data.resample(
            self.bar_length, label='right').last().ffill().iloc[:-1]
        self.data['mid'] = self.data.mean(axis=1)

        if len(self.data) > self.min_length:
            self.min_length += 1
            x = np.arange(self.reg_length)
            reg = np.polyfit(x, self.data['mid'].iloc[-self.reg_length:], deg=1)
            x_ = np.arange(len(x) + 1)
            pred = np.polyval(reg, x_)[-1]
            position = 1 if pred > self.data['mid'].iloc[-1] else -1
            if position == 1:
                if self.position == 0:
                    self.create_order(self.instrument, self.units)
                elif self.position == -1:
                    self.create_order(self.instrument, self.units * 2)
                self.position = 1
            elif position == -1:
                if self.position == 0:
                    self.create_order(self.instrument, -self.units)
                elif self.position == 1:
                    self.create_order(self.instrument, -self.units * 2)
                self.position = -1


if __name__ == '__main__':
    strat = 2
    if strat == 1:
        rt = RegressionTrader('../pyalgo.cfg', 'DE30_EUR', '5s', 3, 1)
        rt.stream_data(rt.instrument, stop=250)
        rt.create_order(rt.instrument, units=-rt.position * rt.units)
    elif strat == 2:
        rt = RegressionTrader('../pyalgo.cfg', instrument='EUR_USD',
                             bar_length='5s', reg_length=4, units=100000)
        rt.stream_data(rt.instrument, stop=200)
        rt.create_order(rt.instrument, units=-rt.position * rt.units)
    else:
        print('Strategy not known.')
