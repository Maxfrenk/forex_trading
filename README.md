# forex_trading
A trading strategy created using the Darwinex ZeroMQ Connector for Python 3 and MetaTrader 4.

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
