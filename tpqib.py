#
# Python Wrapper Class for
# Interactive Brokers API
# based on IbPy
#
# Python for Algorithmic Trading
# (c) Dr. Yves J. Hilpisch
# The Python Quants GmbH
#
# Author: Michael Schwed

from ib.ext.Contract import Contract
from ib.ext.Order import Order
from ib.ext.TickType import TickType as TickType
from ib.opt.dispatcher import Dispatcher
from ib.opt.receiver import Receiver
from ib.opt.sender import Sender

import ib.opt
from time import sleep
import datetime as dt
import pandas as pd
import warnings

__version__ = '0.0.8'

EXCLUDE = ['contractDetails', 'contractDetailsEnd', 'position', 'positionEnd',
           'error', 'accountSummary', 'openOrder', 'orderStatus',
           'execDetails', 'commissionReport', 'connectionClosed',
           'nextValidId', 'tickGeneric', 'tickString', 'tickSnapshotEnd',
           'tickPrice', 'tickSize', 'historicalData']

VALID_BAR_SIZES = ['1 sec', '5 secs', '10 secs', '15 secs', '30 secs',
                   '1 min', '2 mins', '3 mins', '5 mins', '10 mins',
                   '15 mins', '20 mins', '30 mins', '1 hour', '2 hours',
                   '3 hours', '4 hours', '8 hours', '1 day', '1 week',
                   '1 month']

WHAT_TO_SHOW = ['TRADES', 'MIDPOINT', 'ASK', 'BID', 'BID_ASK',
                'HISTORICAL_VOLATILITY', 'OPTION_IMPLIED_VOLATILITY',
                'REBATE_RATE', 'FREE_RATE']

STREAMING_DATA_TYPES = ['lastTimestamp', 'askPrice', 'askSize',
                        'bidPrice', 'bidSize', 'low', 'high', 'close',
                        'volume', 'lastPrice', 'lastSize', 'halted']

ORDER_TYPES = ['MKT', 'MIT', 'MOC', 'MTL', 'MKT PRT', 'LMT', 'LOC', 'LIT',
               'PEG MKT', 'PEG STK', 'REL', 'STP', 'STP LMT', 'STP PRT']


class ServerError(Exception):
    pass

class tpqibcon(ib.opt.Connection):
    def __init__(self, host='127.0.0.1', port=7497, clientId=0):
        dispatcher = Dispatcher() 
        receiver = Receiver(dispatcher)
        sender = Sender(dispatcher)
        super(tpqibcon, self).__init__(
            host, port, clientId, receiver, sender, dispatcher)
        self.reported_orders = set()
        self.next_request_id = 1
        self.ticker_data = dict()
        self.tick_callbacks = dict()
        self.only_one_tick = set()
        self.hist_data_loading = dict()
        self.hist_data = dict()
        self.open_temp_requests = dict()
        self.temp_requests = dict()
        self.error_requests = dict()
        try:
            self.connect()
        except:
            raise IOError("Can not connect %s on port %s" % (host, port))
        sleep(1)
        self.__register_handlers__()
        self.reqIds(-1)

    def create_contract(self, symbol, sec_type, exch, prim_exch, curr):
        """ Deprecated, please use one of create_stock(), instead. 

        Create a contract object defining what will
        be purchased, at which exchange and in which currency.

        Arguments:
        
        symbol, string: 
            the ticker symbol for the contract.
        sec_type, string:
            the security type for the contract ('STK' is 'stock').
        exch, string: 
            the exchange to carry out the contract on.
        prim_exch, string:
            the primary exchange to carry out the contract on.
        curr, string:
            the currency in which to purchase the contract.

        Return:
            the contract object.
        """
        dep_msg = """Deprecated, please use one of create_stock, create_index, 
        create_fx, create_cfd or create_future instead."""
        self.__deprecated_warning__(dep_msg)
        contract = Contract()
        contract.m_symbol = symbol
        contract.m_secType = sec_type
        contract.m_exchange = exch
        contract.m_primaryExch = prim_exch
        contract.m_currency = curr
        return contract

    def create_stock(self, symbol, exch, curr, prim_exch=None):
        """ Create a stock contract object defining what will
        be purchased, at which exchange and in which currency.

        Arguments:
        
        symbol, string: 
            the ticker symbol for the contract.
        exch, string: 
            the exchange to carry out the contract on.
        curr, string:
            the currency in which to purchase the contract.

        Return:
            the contract object.
        """

        contract = Contract()
        contract.m_symbol = symbol
        contract.m_secType = "STK"
        contract.m_currency = curr
        contract.m_exchange = exch
        if prim_exch == None:
            prim_exch = exch
        contract.m_primaryExch = prim_exch
        return contract

    def create_fx(self, symbol, exch, curr):
        """ Create a fx contract object defining what will
        be purchased, at which exchange and in which currency.

        Arguments:
        
        symbol, string: 
            the ticker symbol for the contract.
        exch, string: 
            the exchange to carry out the contract on.
        curr, string:
            the currency in which to purchase the contract.

        Return:
            the contract object.
        """

        contract = Contract()
        contract.m_symbol = symbol
        contract.m_secType = "CASH"
        contract.m_currency = curr
        contract.m_exchange = exch
        return contract

    def create_index(self, symbol, exch, curr):
        """ Create an index contract object defining what will
        be purchased, at which exchange and in which currency.

        Arguments:
        
        symbol, string: 
            the ticker symbol for the contract.
        exch, string: 
            the exchange to carry out the contract on.
        curr, string:
            the currency in which to purchase the contract.

        Return:
            the contract object.
        """
        contract = Contract()
        contract.m_symbol = symbol
        contract.m_secType = "IND"
        contract.m_currency = curr
        contract.m_exchange = exch
        return contract

    def create_cfd(self, symbol, exch, curr):
        """ Create a cfd contract object defining what will
        be purchased, at which exchange and in which currency.

        Arguments:
        
        symbol, string: 
            the ticker symbol for the contract.
        exch, string: 
            the exchange to carry out the contract on.
        curr, string:
            the currency in which to purchase the contract.

        Return:
            the contract object.
        """
        contract = Contract()
        contract.m_symbol = symbol
        contract.m_secType = "CFD"
        contract.m_currency = curr
        contract.m_exchange = exch
        return contract

    def create_future(self, symbol, exch, curr, expiry, include_expired=False):
        """ Create a future contract object defining what will
        be purchased, at which exchange and in which currency.

        Arguments:
        
        symbol, string: 
            the ticker symbol for the contract.
        exch, string: 
            the exchange to carry out the contract on.
        curr, string:
            the currency in which to purchase the contract.

        Return:
            the contract object.
        """

        if isinstance(expiry, str):
            pass
        elif isinstance(expiry, dt.datetime) or isinstance(expiry, dt.date):
            expiry =expiry.strftime('%Y%m')
        else:
            msg = "start must either be a datetime object or a string"
            msg += " in format 'YYYYMM'."
            raise ValueError(msg)

        contract = Contract()
        contract.m_symbol = symbol
        contract.m_secType = "FUT"
        contract.m_currency = curr
        contract.m_exchange = exch
        contract.m_expiry = expiry
        
        if include_expired is not True:
            include_expired = False
        contract.m_includeExpired = include_expired
        return contract

    def contract_details(self, contract, kind='dataframe'):
        """ Return the details of a contract object.

        Arguments:

        contract: an instance of Contract as returned by create_stock, 
            create_index and so on.

        kind, 'list' or 'dataframe (default): defines the shape of the returned
            result, either as list of dictinoarys or as pandas.DataFrame.  

        Returns:
            the details of the contract object.
        """
        if not isinstance(contract, Contract):
            raise ValueError('contract must be a Contract object')

        if kind not in ('dataframe', 'list'):
            raise ValueError("kind must be 'list' of 'dataframe'")
         
        req_id = self.next_request_id 
        self.next_request_id += 1
        self.reqContractDetails(req_id, contract)

        
        while ((req_id not in self.temp_requests) and 
               (req_id not in self.error_requests)):
            sleep(0.2)
            
        if req_id in self.error_requests:
            raise ServerError(self.error_requests[req_id])

        data = self.temp_requests[req_id]
        del self.temp_requests[req_id]

        data_parsed = [self.__parse_contract_details__(d) for d in data]
        
        if kind == 'dataframe':
            ret = pd.DataFrame(data_parsed)
        else:
            ret = data_parsed
        return ret

    def contract_summary(self, contract, kind='dataframe'):
        """ Return the details of a contract object.

        Arguments:

        contract: an instance of Contract as returned by create_stock, 
            create_index and so on.

        kind, 'list' or 'dataframe (default): defines the shape of the returned
            result, either as list of dictinoarys or as pandas.DataFrame.  

        Returns:
            the details of the contract object.
        """
        if not isinstance(contract, Contract):
            raise ValueError('contract must be a Contract object')

        if kind not in ('dataframe', 'list'):
            raise ValueError("kind must be 'list' of 'dataframe'")
         
        req_id = self.next_request_id 
        self.next_request_id += 1

        self.reqContractDetails(req_id, contract)
        while ((req_id not in self.temp_requests) and 
               (req_id not in self.error_requests)):
            sleep(0.2)
            
        if req_id in self.error_requests:
            raise ServerError(self.error_requests[req_id])
            
        data = self.temp_requests[req_id]
        del self.temp_requests[req_id]

        data_parsed = [self.__parse_contract_summary__(d) for d in data]
        

        if kind == 'dataframe':
            ret = pd.DataFrame(data_parsed)
        else:
            ret = data_parsed
        return ret

 

    def create_order(self, order_type, quantity, action):
        """ Deprecated, please use one of create_stock, create_index, 
        create_fx, create_cfd or create_future instead.

        Create an Order object (Market/Limit) to go long/short.

        Arguments:

        order_type, string:
            one of 'MKT' or 'LMT' for market or limit orders.
        quantity, int: 
            number of units to order.
        action, string:
            one of  'BUY' or 'SELL'.
        """

        #dep_msg = """Deprecated, please use one of create_stock, create_index, 
        #create_fx, create_cfd or create_future instead."""
        #self.__deprecated_warning__(dep_msg)

        if order_type not in ['MKT', 'LMT']:
            raise ValueError("order_type must be one of 'MKT' or 'LMT'.")
        if action not in ['BUY', 'SELL']:
            raise ValueError("action must be one of 'BUY' or 'SELL'.")
        order = Order()
        order.m_orderType = order_type
        order.m_totalQuantity = quantity
        order.m_action = action
        return order

    ### Order object

    def create_auction_order(self, action, quantity, price):
        """ Create an auction order. 
        More about the different order types can be found here:
        https://interactivebrokers.github.io/tws-api/basic_orders.html

        Arguments:

        action, string:
            one of  'BUY' or 'SELL'.
        quantity, float: 
            number of units to order.
        price, float:
            the price of the oreder.
        """
        
        order = self.__create_order__(action, quantity, 'MTL', limit_price=price)
        order.m_tif = "AUC"
        return order

    def create_limit_order(self, action, quantity, limit_price):
        """ Create a limit order. 
        More about the different order types can be found here:
        https://interactivebrokers.github.io/tws-api/basic_orders.html

        Arguments:

        action, string:
            one of  'BUY' or 'SELL'.
        quantity, float: 
            number of units to order.
        limit_price, float:
            the limit price of the order.

        Return:
            the order object.
        """
        order = self.__create_order__(action, quantity, 'LMT',
                                      limit_price=limit_price)
        return order

    def create_limit_if_touched_order(self, action, quantity, limit_price, 
                                      trigger_price):
        """ Create a limit if touched order. 
        More about the different order types can be found here:
        https://interactivebrokers.github.io/tws-api/basic_orders.html

        Arguments:

        action, string:
            one of  'BUY' or 'SELL'.
        quantity, float: 
            number of units to order.
        limit_price, float:
            the limit price of the order.
        trigger_price, float:
            the price to trigger the order.

        Return:
            the order object.
        """
        
        try:
            trigger_price = float(trigger_price)
        except:
            raise ValueError('trigger_price must be a number.')
        order = self.__create_order__(action, quantity, 'LIT', 
                                      limit_price=limit_price,
                                      aux_price=trigger_price)
        order.m_action = action
        return order

    def create_limit_on_close_order(self, action, quantity, limit_price):
        """ Create a limit on close order. 
        More about the different order types can be found here:
        https://interactivebrokers.github.io/tws-api/basic_orders.html

        Arguments:

        action, string:
            one of  'BUY' or 'SELL'.
        quantity, int: 
            number of units to order.
        limit_price, float:
            the limit price of the order.

        Return:
            the order object.
        """
        
        order = self.__create_order__(action, quantity, 'LOC',
                                      limit_price=limit_price)
        return order

    def create_limit_on_open_order(self, action, quantity, limit_price):
        """ Create a limit on open order. 
        More about the different order types can be found here:
        https://interactivebrokers.github.io/tws-api/basic_orders.html

        Arguments:

        action, string:
            one of  'BUY' or 'SELL'.
        quantity, int: 
            number of units to order.
        limit_price, float:
            the limit price of the order.

        Return:
            the order_object.
        
        """
        
        order = self.__create_order__(action, quantity, 'LMT',
                                      limit_price=limit_price)
        order.m_tif = "OPG"
        return order

    def create_market_order(self, action, quantity):
        """ Create a market order. 
        More about the different order types can be found here:
        https://interactivebrokers.github.io/tws-api/basic_orders.html

        Arguments:

        action, string:
            one of  'BUY' or 'SELL'.
        quantity, float: 
            number of units to order.

        Return:
            the order object.
        """
        
        order = self.__create_order__(action, quantity, 'MKT')
        return order

    def create_market_if_touched_order(self, action, quantity, price):
        """ Create a market if touched order. 
        More about the different order types can be found here:
        https://interactivebrokers.github.io/tws-api/basic_orders.html

        Arguments:

        action, string:
            one of  'BUY' or 'SELL'.
        quantity, float: 
            number of units to order.
        price, float:
            the price of the order.

        Return:
            the order object.
        """
        
        try:
            price = float(price)
        except:
            raise ValueError('price must be a number.')
        order = self.__create_order__(action, quantity, 'MIT',
                                      aux_price=price)
        return order

    def create_market_on_close_order(self, action, quantity):
        """ Create a market on close order. 
        More about the different order types can be found here:
        https://interactivebrokers.github.io/tws-api/basic_orders.html

        Arguments:

        action, string:
            one of  'BUY' or 'SELL'.
        quantity, int: 
            number of units to order.
        
        Return:
            the order object.
        """
        
        order = self.__create_order__(action, quantity, 'MOC')
        return order

    def create_market_on_open_order(self, action, quantity):
        """ Create a market on opern order. 
        More about the different order types can be found here:
        https://interactivebrokers.github.io/tws-api/basic_orders.html

        Arguments:

        action, string:
            one of  'BUY' or 'SELL'.
        quantity, int: 
            number of units to order.
        
        Return:
            the order object.
        """
          
        order = self.__create_order__(action, quantity, 'MKT')
        order.m_tif = "OPG"
        return order

    def create_market_to_limit_order(self, action, quantity):
        """ Create a market to limit order. 
        More about the different order types can be found here:
        https://interactivebrokers.github.io/tws-api/basic_orders.html

        Arguments:

        action, string:
            one of  'BUY' or 'SELL'.
        quantity, float: 
            number of units to order.

        Return:
            the order object.
        """
        
        order = self.__create_order__(action, quantity, 'MTL')
        return order

    def create_market_with_protection_order(self, action, quantity):
        """ Create a market with protection order. 
        More about the different order types can be found here:
        https://interactivebrokers.github.io/tws-api/basic_orders.html

        Arguments:

        action, string:
            one of  'BUY' or 'SELL'.
        quantity, float: 
            number of units to order.

        Return:
            the order object.
        """
        
        order = self.__create_order__(action, quantity, 'MKT PRT')
        return order


    def create_pegged_to_market_order(self, action, quantity, market_offset):
        """ Create a pegged to market order. 
        More about the different order types can be found here:
        https://interactivebrokers.github.io/tws-api/basic_orders.html

        Arguments:

        action, string:
            one of  'BUY' or 'SELL'.
        quantity, float: 
            number of units to order.
        market_offset, float:
            the offset price of the order.

        Return:
            the order object.
        """
        
        try:
            price = float(market_offset)
        except:
            raise ValueError('market_offset must be a number.')
        order = self.__create_order__(action, quantity, 'PEG MKT',
                                      aux_price=price)
        return order

    def create_pegged_to_stock_order(self, action, quantity, delta, 
                                     stock_ref_price, starting_price):
        """ Create a pegged to stock order. 
        More about the different order types can be found here:
        https://interactivebrokers.github.io/tws-api/basic_orders.html

        Arguments:

        action, string:
            one of  'BUY' or 'SELL'.
        quantity, float: 
            number of units to order.
        delta, float:
            the delta that is added to the reference price of the underlying.
        stock_ref_price, float:
            the reference price of the underlying.
        starting_price: float:
            the starting price of the order.

        Return:
            the order object.
        """
        
        try:
            delta = float(delta)
        except:
            raise ValueError('delta must be a number.')
        try:
            starting_price = float(starting_price)
        except:
            raise ValueError('starting_price must be a number.')
        try:
            stock_ref_price = float(stock_ref_price)
        except:
            raise ValueError('stock_ref_price must be a number.')
        order = self.__create_order__(action, quantity, 'PEG STK',
                                      limit_price=stock_ref_price)
        order.m_action = action
        order.m_delta = delta
        order.m_startingPrice = starting_price
        return order

    def create_pegged_to_primary_order(self, action, quantity, offset_amount, 
                                       price_cap=None):
        """ Create a pegged to primary order. 
        More about the different order types can be found here:
        https://interactivebrokers.github.io/tws-api/basic_orders.html

        Arguments:

        action, string:
            one of  'BUY' or 'SELL'.
        quantity, float: 
            number of units to order.
        offset_amount: float:
            additional amount added to the national best bid or offer. 
        price_cap, float or None (default) :
            the cap of of the order price.

        Return:
            the order object.
        """
        
        try:
            offset_amount = float(offset_amount)
        except:
            raise ValueError('offset_amount must be a number.')
        if price_cap is not None:    
            try:
                price_cap = float(price_cap)
            except:
                raise ValueError('price_cap must be a number.')
        order = self.__create_order__(action, quantity, 'REL',
                                      aux_price=offset_price)
        if price_cap is not None:
            order.m_lmtPrice = price_cap
        
        return order

    def create_stop_order(self, action, quantity, stop_price):
        """ Create a stop order. 
        More about the different order types can be found here:
        https://interactivebrokers.github.io/tws-api/basic_orders.html

        Arguments:

        action, string:
            one of  'BUY' or 'SELL'.
        quantity, float: 
            number of units to order.
        stop_price, float:
            the stop price of the order.

        Return:
            the order object.
        """
        
        try:
            stop_price = float(stop_price)
        except:
            raise ValueError('stop_price must be a number.')

        order = self.__create_order__(action, quantity, 'STP',
                                      aux_price=stop_price)
        return order

    def create_stop_limit_order(self, action, quantity, stop_price,
                                limit_price):
        """ Create a stop limit order. 
        More about the different order types can be found here:
        https://interactivebrokers.github.io/tws-api/basic_orders.html

        Arguments:

        action, string:
            one of  'BUY' or 'SELL'.
        quantity, float: 
            number of units to order.
        stop_price, float:
            the stop price of the order.

        Return:
            the order object.
        """
        
        try:
            stop_price = float(stop_price)
        except:
            raise ValueError('stop_price must be a number.')
        try:
            limit_price = float(limit_price)
        except:
            raise ValueError('limit_price must be a number.')
        
        order = self.__create_order__(action, quantity, 'STP LMT',
                                      limit_price=limit_price, 
                                      aux_price=stop_price)
        return order

    def create_stop_with_protection_order(self, action, quantity, stop_price):
        """ Create a stop with protection order. 
        More about the different order types can be found here:
        https://interactivebrokers.github.io/tws-api/basic_orders.html

        Arguments:

        action, string:
            one of  'BUY' or 'SELL'.
        quantity, float: 
            number of units to order.
        stop_price, float:
            the stop price of the order.

        Return:
            the order object.
        """
        try:
            stop_price = float(stop_price)
        except:
            raise ValueError('stop_price must be a number.')
        order = self.__create_order__(action, quantity, 'STP PRT',
                                      aux_price=stop_price)
        return order

    def create_sweep_to_fill_order(self, action, quantity, limit_price):
        """ Create a sweep to fill order. 
        More about the different order types can be found here:
        https://interactivebrokers.github.io/tws-api/basic_orders.html

        Arguments:

        action, string:
            one of  'BUY' or 'SELL'.
        quantity, float: 
            number of units to order.
        limit_price, float:
            the limit_price of the order.

        Return:
            the order object.
        """
        try:
            limit_price = float(limit_price)
        except:
            raise ValueError('limit_price must be a number.')
        order = self.__create_order__(action, quantity, 'LMT',
                                      limit_price=limit_price)
        order.m_sweepToFill = True
        return order



    def place_order(self, contract, order):
        ''' Place the order for a given contract via TWS
        '''
        if not isinstance(contract, Contract):
            raise ValueError('contract must be a Contract object')
        if not isinstance(order, Order):
            raise ValueError('order must be an Order object')

        self.placeOrder(self.next_request_id, contract, order)
        self.next_request_id += 1

    def req_positions(self):
        ''' Wrapper to request from TWS a list of positions
        '''
        self.reqPositions()

    # setter and getter methods

    def set_next_order_id(self, order_id):
        ''' Sets the next order id, either when creating a new connection
        or when sending an open order
        '''
        self.next_request_id = order_id

    def get_next_order_id(self):
        return self.next_request_id

    def current_market_data(self, contract, kind='dataframe'):
        """ Send an one time request for market data of a contract
        object.

        Argument:

        contract, of type Contract:
            the contract object to get market data for.

        kind: , 'list' or 'dataframe (default): defines the shape of the returned
            result, either as list of dictinoarys or as pandas.DataFrame.  

        Returns:
            the current market data for the contract object.

        """
        if not isinstance(contract, Contract):
            raise ValueError('contract must be a Contract object')

        if kind not in ('dataframe', 'list'):
            raise ValueError("kind must be 'list' of 'dataframe'")
         
        req_id = self.next_request_id 
        self.next_request_id += 1

        self.only_one_tick.add(req_id)
        self.reqMktData(req_id, contract, '', True)

        while ((req_id not in self.temp_requests) and 
               (req_id not in self.error_requests)):
            sleep(0.2)
            
        if req_id in self.error_requests:
            raise ServerError(self.error_requests[req_id])
            
        data = self.temp_requests[req_id]
        del self.temp_requests[req_id]
        self.only_one_tick.remove(req_id)

        #data_parsed = [self.__parse_contract_summary__(d) for d in data]
        

        if kind == 'dataframe':
            ret = pd.DataFrame(data, index=[0,])
        else:
            ret = data
        return ret
        

    def request_market_data(self, contract, callback=None):
        ''' Sends a market data request for a contract to TWS,
        the response is streamed to the callback function until
        the request is canceled with self.cancel_market_data(request_id);
        if no callback is given, the results will be printed;
        returns the request_id
        '''
        self.tick_callbacks[self.next_request_id] = callback
        self.reqMktData(self.next_request_id, contract, '', False)
        old = self.next_request_id
        self.next_request_id += 1
        return old

    def cancel_market_data(self, request_id):
        ''' Cancels a running market data request '''
        self.cancelMktData(request_id)

    def request_historical_data(self, contract, end_date_time, duration,
                                bar_size, what_to_show, use_RTH=True,
                                format_date=1):
        ''' Sends a historical data request for contract to the TWS,
        the response is streamed to the callback function until the request
        is canceld with self.cancel_market_data(request_id);
        returns the request_id
        '''
        if bar_size not in VALID_BAR_SIZES:
            raise ValueError('bar_size must be one of %s' % VALID_BAR_SIZES)
        if type(use_RTH) != bool:
            raise TypeError('use_RTH must be of type bool')
        elif use_RTH is True:
            use_RTH = True
        else:
            use_RTH = False
        if type(end_date_time) != dt.datetime:
            raise TypeError('end_date_time must be of type datetime')
        else:
            end_date_time = end_date_time.strftime('%Y%m%d %X')
        if what_to_show not in WHAT_TO_SHOW:
            raise ValueError('what_to_show must be in %s' % WHAT_TO_SHOW)

        self.reqHistoricalData(self.next_request_id, contract,
                               endDateTime=end_date_time, durationStr=duration,
                               barSizeSetting=bar_size, whatToShow=what_to_show,
                               useRTH=False, formatDate=format_date)

        # count request id

        old = self.next_request_id
        self.next_request_id += 1

        # set loading flag for request id

        self.hist_data_loading[old] = True
        return old

    def get_historical_data(self, request_id, silent=False):
        ''' Returns the historical data for a given request id,
        if silent equal to True, the 'is still loading' message will be ommited
        '''

        if request_id not in self.hist_data_loading:
            # unknown request id
            raise ValueError('No data found for request id %s' % request_id)
        elif self.hist_data_loading[request_id] is True:
            # still loading
            if not silent:
                print("Historical data for request id %s still loading" %
                      request_id)
            return False
        else:
            return self.hist_data[request_id]

    def is_historical_data_loading(self, request_id):
        ''' Returns True if the request with id request_id is still loading
        and False else
        '''
        if request_id not in self.hist_data_loading:
            # unknown request id
            raise ValueError('No data found for request id %s' % request_id)
        elif self.hist_data_loading[request_id] is True:
            # still loading
            return True
        else:
            return False

    # callback functions for requests to TWS

    def on_position_request(self, msg):
        ''' Callback for position requests
        '''
        con = msg.contract
        pos = msg.pos
        avgCost = msg.avgCost
        templ = 'Quantity: %s, Symbol: %s, Currency: %s, Average Cost: %s'
        out = templ % (pos, con.m_symbol, con.m_currency, avgCost)
        print(out)

    def on_account_summary(self, msg):
        ''' Callback for account summary
        '''
        print('%s: %s' % (msg.tag, msg.value))

    def on_open_order(self, msg):
        ''' Callback for open orders
        '''
        con = msg.contract
        order = msg.order
        if msg.orderId not in self.reported_orders:
            self.reported_orders.add(msg.orderId)
            print('Order: %s %s %s' %
                  (order.m_action, order.m_totalQuantity, con.m_symbol))

    def on_order_status(self, msg):
        ''' Callback for order status
        '''
        print('    Filled: %s, Status: %s' % (msg.filled, msg.status))

    def on_commission_report(self, msg):
        pass



    def on_next_order_id(self, msg):
        ''' Callback for next order id requests
        '''
        self.set_next_order_id(msg.orderId)



    def on_hist_data(self, msg):
        data_id = msg.reqId
        # columns = msg.keys()
        new_data = dict()
        for item in msg.items():
            if item[0] != 'reqId':
                if item[0] == 'date':
                    date = item[1]
                    if date[:8] == 'finished':
                        self.hist_data[data_id] = self.hist_data[
                            data_id].set_index('date')
                        self.hist_data_loading[data_id] = False
                        return True
                    elif len(date) == 8:
                        value = dt.datetime(
                            int(date[:4]), int(date[4:6]), int(date[6:]))
                    else:
                        time = date[9:].split(":")
                        value = dt.datetime(int(date[:4]), int(date[4:6]), int(
                          date[6:8]), int(time[0]), int(time[1]), int(time[2]))
                else:
                    value = item[1]

                new_data[item[0]] = value
        if data_id not in self.hist_data:
            temp_data = pd.DataFrame([new_data])
        else:
            temp_data = self.hist_data[data_id].append(
                new_data, ignore_index=True)
        self.hist_data[data_id] = temp_data


    def reply_handler(self, msg):
        ''' Callback for all other messages
        '''
        if msg.typeName not in EXCLUDE:
            print('Server Response: %s, %s' % (msg.typeName, msg))

    def __deprecated_warning__(self, msg):
        with warnings.catch_warnings():
            warnings.simplefilter("always")
            warnings.warn(msg, DeprecationWarning)

    # Callback functions for incoming messages from TWS

    def __register_handlers__(self):
        self.register(self.on_next_order_id, 'NextValidId')
        self.register(self.__on_contract_details__, 'ContractDetails')
        self.register(self.__on_contract_details_end__, 'ContractDetailsEnd')
        self.register(self.on_position_request, 'Position')
        self.register(self.on_account_summary, 'AccountSummary')
        self.register(self.on_open_order, 'OpenOrder')
        self.register(self.on_order_status, 'OrderStatus')
        self.register(self.on_commission_report, 'CommissionReport')
        self.register(self.__on_connection_closed__, 'ConnectionClosed')
        self.register(self.__on_tick__, 'TickSize')
        self.register(self.__on_tick__, 'TickPrice')
        self.register(self.__on_tick__, 'TickGeneric')
        self.register(self.__on_tick__, 'TickString')
        self.register(self.__on_snapshot_end__, 'TickSnapshotEnd')
        self.register(self.on_hist_data, 'HistoricalData')
        self.register(self.__error_handler__, 'Error')
        self.registerAll(self.reply_handler)

    def __create_order__(self, action, quantity, order_type,
                         aux_price=None, limit_price=None, price=None):
        """ Create a prototype of an order. 
        
        Arguments:

        action, string:
            one of  'BUY' or 'SELL'.
        quantity, float: 
            number of units to order.
        order_type, string:
            the type of the order.

        Return:
            a prototype for an order.
        """
        
        if action not in ['BUY', 'SELL']:
            raise ValueError("action must be one of 'BUY' or 'SELL'.")
        try:
            quantity = int(quantity)
        except:
            raise TypeError('quantity must be an integer.')
        if order_type not in ORDER_TYPES:
            raise ValueError('Unknown order type')
        if aux_price is not None:
            try:
                aux_price = float(aux_price)
            except:
                raise ValueError('aux_price must be a number.')
        if limit_price is not None:
            try:
                limit_price = float(limit_price)
            except:
                raise ValueError('limit_price must be a number.')
        if price is not None:
            try:
                price = float(price)
            except:
                raise ValueError('price must be a number.')
        order = Order()
        order.m_action = action
        order.m_orderType = order_type
        order.m_totalQuantity = quantity
        if aux_price is not None:
            order.m_auxPrice = aux_price
        if limit_price is not None:  
            order.m_lmtPrice = limit_price
        return order

    def __error_handler__(self, msg):
        """ Callback for errors from server """
        if hasattr(msg, 'id') and msg.id != -1:
            if msg.id in self.hist_data_loading:
                self.hist_data_loading[int(msg.id)] = False
                self.hist_data[int(msg.id)] = pd.DataFrame()

                print('Server Error: %s' % msg.errorMsg)
            else:
                self.error_requests[msg.id] = msg.errorMsg
        if not hasattr(msg, 'id') or msg.id == -1:
            if hasattr(msg, 'errorMsg'):
                e_msg = msg.errorMsg
            else:
                e_msg = 'Unknown error'
            print("msg", e_msg)
            raise ServerError(e_msg)

    def __on_connection_closed__(self, msg):
        """ Callback for closing connection
        """
        print('Bye.')

    def __on_contract_details__(self, msg):
        """Callback method for incoming contract_detaiuls messages."""
  
        back = dict()
        req_id = msg.reqId 
            
        if req_id not in self.open_temp_requests:
            self.open_temp_requests[req_id] = list()
        self.open_temp_requests[req_id].append(msg)

    def __on_contract_details_end__(self, msg):
        """Callback method for incoming contract_details_end messages."""
        
        req_id = msg.reqId
        data = self.open_temp_requests[req_id]
        self.temp_requests[req_id] = data
        del self.open_temp_requests[req_id]

    def __on_snapshot_end__(self, msg):
        """ Callback for incoming snapshots """
        tick_id = msg.reqId
        tick_data = self.open_temp_requests[tick_id]
        if 'lastTimestamp' in tick_data:
            tick_data['lastTimestamp'] = dt.datetime.fromtimestamp(int(tick_data['lastTimestamp']))
        self.temp_requests[tick_id] = tick_data
        #del self.open_temp_requests[tick_id]

    def __on_tick__(self, msg):
        """ Callback for market data requests"""
       
        tick_id = msg.tickerId
        if msg.typeName == 'tickSize':
            field_name = TickType.getField(msg.field)
            value = msg.size
        elif msg.typeName == 'tickPrice':
            field_name = TickType.getField(msg.field)
            value = msg.price
        elif msg.typeName in ['tickGeneric', 'tickString']:
            field_name = TickType.getField(msg.tickType)
            value = msg.value
        else:
            print('Found unknown type in tick data: %s' %msg.typeName)

        if tick_id in self.only_one_tick:
            if tick_id not in self.open_temp_requests:
                ticker_data = dict()
            else:
                ticker_data = self.open_temp_requests[tick_id]

            ticker_data[field_name] = value
            self.open_temp_requests[tick_id] = ticker_data
        elif tick_id in self.tick_callbacks:
            self.tick_callbacks[tick_id](field_name, value)


        

    def __parse_contract_details__(self, msg):
        """ Parse the contractDetails object into a dict."""

        ret = dict()
  
        atts = [a for a in dir(msg.contractDetails) if a.startswith('m_') and a != 'm_contractSummary']
        for a in atts:
            ret[a[2:]] =  getattr(msg.contractDetails, a)
        return ret

    def __parse_contract_summary__(self, msg):
        """ Parse the contractSummary object into a dict."""

        ret = dict()
        summary = msg.contractDetails.m_summary
        atts = [a for a in dir(summary) if a.startswith('m_') and a != 'm_contractSummary']
        for a in atts:
            ret[a[2:]] =  getattr(summary, a)
        return ret


    # Deprecated methods, only hold for compability with older versions

    def req_contract_details(self, contract):
        """ Deprecated, please use contract_details instead
        Wrapper to request from TWS details of a contract
        """
        self.__deprecated_warning__('Deprecated, please use contract_details() instead')
        if not isinstance(contract, Contract):
            raise ValueError('contract must be a Contract object')

        self.reqContractDetails(1, contract)


    def get_market_data_once(self, contract):
        """Deprecated, use current_market_data instead. 
        Send an one time request for market data of a contract
        object.

        Argument:

        contract, of type Contract:
            the contract object to get market data for.

        kind: , 'list' or 'dataframe (default): defines the shape of the returned
            result, either as list of dictinoarys or as pandas.DataFrame.  

        Returns:
            the current market data for the contract object.

        """

        dep_msg = """Deprecated, please use current_market_data instead."""
        self.__deprecated_warning__(dep_msg)
