# -*- coding: utf-8 -*-
"""
    moving_avg_trader.py
    
    An trading strategy created using the Darwinex ZeroMQ Connector
    for Python 3 and MetaTrader 4.
    
    Source code:
    https://github.com/Maxfrenk/forex_trading/blob/master/moving_avg_trader.py
    
    The strategy launches 'n' threads (each representing a trader responsible
    for trading one instrument - EURUSD, USDJPY, etc.)
    
    Each trader must:

        1) Extract close price from a market data snapshot every N=10 seconds

        2) Compute moving weighted averages from market data snapshot with spans 
            of 10 and 20 points

        3) If short span values are above long span values throughout most of the 
            data window length:
                
            a) Close existing SELL trades for this symbol 
            b) Open a BUY trade
                   
        4) If short span values are below long span values throughout most of the 
            data window length:
                
            a) Close existing BUY trades for this symbol 
            b) Open a SELL trade
                   
        5) Keep trading until the market is closed (_market_open = False)
    --
    
    @author: Max Frenkel
    
"""

#import os

#############################################################################
#############################################################################
#_path = r'C:\Users\Max\source\repos\dwx-zeromq-connector\v2.0.1\python'
#os.chdir(_path)
#############################################################################
#############################################################################

from examples.template.strategies.base.DWX_ZMQ_Strategy import DWX_ZMQ_Strategy

from pandas import Timedelta, Timestamp, DataFrame, to_datetime
from threading import Thread, Lock
from time import sleep
import random

class moving_average_trader(DWX_ZMQ_Strategy):
    
    def __init__(self, _name="MOVING_AVERAGE_TRADER",
                 _symbols=[
                        #('USDCHF',0.01),
                           ('USDJPY',0.01),
                        #   ('NDX',0.10),
                        #   ('UK100',0.1)
                        #   ('GDAXI',0.01),
                        #   ('XTIUSD',0.01),
                           ('EURUSD',0.01),
                        #   ('SPX500',1.0),
                        #   ('STOXX50E',0.10),
                        #   ('GBPUSD',0.01)
                            ],
                 # Sleep time between cycles, needed in order to give time to collect more market data
                 _delay=10,       # in seconds
                 _broker_gmt=3,    # EDT time zone (GMT - 4)
                 _verbose=False):
        
        super().__init__(_name,
                         _symbols,
                         _broker_gmt,
                         _verbose)
        
        # This strategy's variables
        self._traders = []
        self._market_open = True
        self._delay = _delay
        self._verbose = _verbose

        # append data to this list of per minute prices as quotes come in
        self._market_data = {}
        
        # lock for acquire/release of ZeroMQ connector
        self._lock = Lock()
        
    ##########################################################################
    
    def run(self):
        
        """
        Logic:
            
            For each symbol in self._symbols:
                
                1) Open a new Market Order every 2 seconds
                2) Close any orders that have been running for 10 seconds
                3) Calculate Open P&L every second
                4) Plot Open P&L in real-time
                5) Lot size per trade = 0.01
                6) SL/TP = 10 pips each
        """
        
        # Launch traders!
        for _symbol in self._symbols:
            
            self._zmq._DWX_MTX_SUBSCRIBE_MARKETDATA_(_symbol[0])
            self._market_data[_symbol[0]] = []

            _t = Thread(name="{}_Trader".format(_symbol[0]),
                        target=self._trader_, args=(_symbol,))
            
            _t.daemon = True
            _t.start()
            
            print('[{}_Trader] Alright, here we go.. Gerrrronimooooooooooo!  ..... xD'.format(_symbol[0]))
            
            self._traders.append(_t)
        
        #print('\n\n+--------------+\n+ LIVE UPDATES +\n+--------------+\n')
        
        # _verbose can print too much information.. so let's start a thread
        # that prints an update for instructions flowing through ZeroMQ
        #self._updater_ = Thread(name='Live_Updater',
        #                       target=self._updater_,
        #                       args=(self._delay,))
        
        #self._updater_.daemon = True
        #self._updater_.start()
        
    ##########################################################################
    
    def _updater_(self, _delay=0.1):
        
        while self._market_open:
            
            # Acquire lock
            with self._lock:
                print('\r{}'.format(str(self._zmq._get_response_())), end='', flush=True)
                        
            sleep(self._delay)
            
    ##########################################################################
    
    def _trader_(self, _symbol):
        
        # Note: Just for this example, only the Order Type is dynamic.
        _default_order = self._zmq._generate_default_order_dict()
        _default_order['_symbol'] = _symbol[0]
        _default_order['_lots'] = _symbol[1]
        _default_order['_SL'] = _default_order['_TP'] = 100
        _default_order['_comment'] = '{}_Trader'.format(_symbol[0])
        
        """
        Default Order:
        --
        {'_action': 'OPEN',
         '_type': 0,
         '_symbol': EURUSD,
         '_price':0.0,
         '_SL': 100,                     # 10 pips
         '_TP': 100,                     # 10 pips
         '_comment': 'EURUSD_Trader',
         '_lots': 0.01,
         '_magic': 123456}
        """

        while self._market_open:
            
            # Acquire lock on _zmq object
            with self._lock:
                            
                if _symbol[0] not in self._zmq._Market_Data_DB:
                    continue

                # Get a snapshot of the market data for this thread's (trader's) symbol
                market_data_snapshot = self._zmq._Market_Data_DB[_symbol[0]].copy()
                
                # Reset cycle if nothing received yet - the very beginning
                if len(market_data_snapshot) == 0:
                    continue

                """# Iterate through snapshot picking off the data when the minutes change
                # signifying the close price for that minute 
                itr = iter(market_data_snapshot)
                prev_key = next(itr)
                prev_ts_key = Timestamp(prev_key)    # the first timestamp in snapshot
                minute_changed = False
                for key in market_data_snapshot.keys():
                    ts_key = Timestamp(key)
                    # Check if the minute changed from the prev timestamp
                    if ts_key.minute != prev_ts_key.minute:
                        # if minute changed, grab the close price from previous minute
                        self._market_data[_symbol[0]].append(market_data_snapshot[prev_key][0])
                        minute_changed = True

                    prev_key = key
                    prev_ts_key = ts_key
                """
                # Reset market data that we are about to process for the next loop iteration
                self._zmq._Market_Data_DB[_symbol[0]] = {}

                # Let the other traders use the mutex while we do some math on thread-safe data

            # extract close price from market data
            for key in market_data_snapshot.keys():
                self._market_data[_symbol[0]].append(market_data_snapshot[key][0])

            #print('market_data_snapshot = ')
            #print(market_data_snapshot)

            # compute moving averages from DataFrame
            # weighted avg that gives more weight to more recent points
            data_frame = DataFrame(self._market_data[_symbol[0]])
            #print('data_frame')
            #print(data_frame)
            #if len(self._market_data[_symbol[0]]) >= 4:
                #avg4 = data_frame.rolling(4).mean()
            avg3 = data_frame.ewm(span=10, adjust=False).mean()
            print('Analyzing ' + _symbol[0])
            #print(_symbol[0] + ' span 10')
            #print(avg3)
            #print(avg3.tail(1))
            #if len(self._market_data[_symbol[0]]) >= 8:
                #avg8 = data_frame.rolling(8).mean()
            avg6 = data_frame.ewm(span=20, adjust=False).mean()   
            #print(_symbol[0] + ' span 20')                 
            #print(avg6)
            #print(avg6.tail(1))

            length_data = len(self._market_data[_symbol[0]])
            #print(_symbol[0] + ' analyzing... ' + str(length_data) + ' data points')
            if length_data < 10:
                continue

            # count which curve is above which on left and right halves in data window
            # left half
            half_len = int(length_data/2)
            avg3_less_than_avg6_left = 0
            avg3_greater_than_avg6_left = 0
            for i in range(1, half_len):
                if avg3.at[i, 0] < avg6.at[i, 0]:
                    avg3_less_than_avg6_left += 1
                elif avg3.at[i, 0] > avg6.at[i, 0]:
                    avg3_greater_than_avg6_left += 1
            # right half
            avg3_less_than_avg6_right = 0
            avg3_greater_than_avg6_right = 0
            for i in range(half_len, length_data):
                if avg3.at[i, 0] < avg6.at[i, 0]:
                    avg3_less_than_avg6_right += 1
                elif avg3.at[i, 0] > avg6.at[i, 0]:
                    avg3_greater_than_avg6_right += 1
            quart_len = int(half_len/2)
            scale_const_left = 1.5   # 1.0 < scale_const < 2.0
            scale_const_right = 1.75   # 1.0 < scale_const < 2.0
            min_sell_buy_time_delta = 60.0  # min seconds allowed between a BUY and a SELL

            # make some trade decisions
            #if avg3.at[length_data - 1, 0] > avg6.at[length_data - 1, 0] and \
            #    avg3.at[10, 0] < avg6.at[10, 0]:
            #if (avg3_greater_than_avg6_right > quart_len and avg3_less_than_avg6_left > quart_len) or \
            if avg3_greater_than_avg6_left > scale_const_left * quart_len and \
                avg3_greater_than_avg6_right > scale_const_right * quart_len:
                # Acquire lock on _zmq object
                with self._lock:

                    # avg3 > avg6 on both the left and the right : trending upward throughout the 
                    # entire data window. Close the open SELL orders for this symbol and open a BUY order
                    _ot = self._reporting._get_open_trades_( '{}_Trader'.format(_symbol[0]), 0.1, 10 )
                    
                    # If SELL order received, close it
                    buyOrderOpen = False
                    if self._zmq._valid_response_(_ot) == True:
                        for i in _ot.index:
                            # dont allow BUY to happen if SELL happened less than 60s ago, signalling volatility
                            if abs(Timedelta((to_datetime('now') + Timedelta(self._broker_gmt,'h')) - \
                                to_datetime(_ot.at[i,'_open_time'])).total_seconds()) < min_sell_buy_time_delta:
                                buyOrderOpen = True
                                break

                            if _ot['_type'][i] == 1:
                                _ret = self._execution._execute_({'_action': 'CLOSE', '_ticket': i,
                                                            '_comment': '{}_Trader'.format(_symbol[0])},
                                                            self._verbose, 0.1, 10)

                                if self._zmq._valid_response_(_ret) == True:
                                    print('Closed ' + _symbol[0])
                                    sleep(0.1)
                                else:
                                    print('Error: ')
                                    print(_ret)
                                    break
                            elif _ot['_type'][i] == 0:
                                # If a BUY order is open already, do nothing
                                buyOrderOpen = True
                            
                    if buyOrderOpen == False:
                        print('BUY ' + _symbol[0] + '!! Time: ' + str(Timestamp.now()))

                        # 0 (OP_BUY) or 1 (OP_SELL)
                        _default_order['_type'] = 0                        
                        # Send instruction to MetaTrader
                        _ret = self._execution._execute_(_default_order, self._verbose, 0.1, 10)
                    
                        # Reset cycle if nothing received
                        if self._zmq._valid_response_(_ret) == False:
                            print('Invalid response: ')
                            print(_ret)
                            break

            #elif avg3.at[length_data - 1, 0] < avg6.at[length_data - 1, 0] and \
            #    avg3.at[10, 0] > avg6.at[10, 0]:
            #elif (avg3_less_than_avg6_right > quart_len and avg3_greater_than_avg6_left > quart_len) or \
            elif avg3_less_than_avg6_left > scale_const_left * quart_len and \
                avg3_less_than_avg6_right > scale_const_right * quart_len:
                # Acquire lock on _zmq object
                with self._lock:

                    # on the left, avg3 > avg6 predominantly, on the right, avg3 < avg6 :
                    # turning downward - bearish! OR avg3 < avg6 on both the left and the right :
                    # trending downward throughout the entire data window.  Close the open BUY orders 
                    # for this symbol and open a SELL order
                    _ot = self._reporting._get_open_trades_( '{}_Trader'.format(_symbol[0]), 0.1, 10 )
                    #sleep(2.0)

                    # If BUY received, close it. 
                    sellOrderOpen = False
                    if self._zmq._valid_response_(_ot) == True:
                        for i in _ot.index:
                            # dont allow SELL to happen if BUY happened less than 60s ago, signalling volatility
                            if abs(Timedelta((to_datetime('now') + Timedelta(self._broker_gmt,'h')) - \
                                to_datetime(_ot.at[i,'_open_time'])).total_seconds()) < min_sell_buy_time_delta:
                                sellOrderOpen = True
                                break

                            if _ot['_type'][i] == 0:
                                _ret = self._execution._execute_({'_action': 'CLOSE', '_ticket': i,
                                                                '_comment': '{}_Trader'.format(_symbol[0])},
                                                                self._verbose, 0.1, 10)
                                
                                if self._zmq._valid_response_(_ret) == True:
                                    print('Closed ' + _symbol[0])
                                    sleep(0.1)
                                else:
                                    print('Error: ')
                                    print(_ret)
                                    break
                            elif _ot['_type'][i] == 1:
                                # If a SELL order is open already, do nothing
                                sellOrderOpen = True

                    if sellOrderOpen == False:
                        print('SELL ' + _symbol[0] + '!! Time: ' + str(Timestamp.now()))

                        # 0 (OP_BUY) or 1 (OP_SELL)
                        _default_order['_type'] = 1
                        # Send instruction to MetaTrader
                        _ret = self._execution._execute_( _default_order, self._verbose, 0.1, 10 )
                    
                        # Reset cycle if nothing received
                        if self._zmq._valid_response_(_ret) == False:
                            print('Invalid response: ')
                            print(_ret)
                            break

            data_len_limit = 30
            # snip off old data to keep things under data_len_limit length
            if length_data >= data_len_limit:
                self._market_data[_symbol[0]] = self._market_data[_symbol[0]][(length_data-data_len_limit):length_data]
            
            # Sleep between cycles
            sleep(self._delay)
            
    ##########################################################################
    
    def stop(self):
        
        self._market_open = False
        
        for _t in self._traders:
        
            # Setting _market_open to False will stop each "trader" thread
            # from doing anything more. So wait for them to finish.
            _t.join()
            
            print('\n[{}] .. and that\'s a wrap! Time to head home.\n'.format(_t.getName()))
        
        # Kill the updater too        
        #self._updater_.join()
        
        #print('\n\n{} .. wait for me.... I\'m going home too! xD\n'.format(self._updater_.getName()))

        # Stop market data feeds        
        for _symbol in self._symbols:
            self._zmq._DWX_MTX_UNSUBSCRIBE_MARKETDATA_(_symbol[0])

        # Send mass close instruction to MetaTrader in case anything's left.
        self._zmq._DWX_MTX_CLOSE_ALL_TRADES_()
        
    ##########################################################################
