from numpy.ma.core import append
from telegram import Bot
from tradingview_ta import TA_Handler, Interval, Exchange
import datetime
import time

# Define the trading pair and exchange
symbols = ["BTCUSD", "ETHUSD", "LTCUSD"]
exchange = "COINBASE"  # Use your preferred exchange

def daily_ohlc_data():
# Fetch TradingView OHLC Data
    for symbol in symbols:
        ta = TA_Handler(
            symbol=symbol,
            exchange=exchange,
            screener="crypto",
            interval=Interval.INTERVAL_1_HOUR,  # Choose the timeframe (e.g., 1h, 1d)
        )
# Get the latest OHLC data
        analysis = ta.get_analysis()
        print("High:", analysis.indicators["high"])
        print("Low:", analysis.indicators["low"])
        high = str(analysis.indicators["high"])
        low = str(analysis.indicators["low"])
#Save the OHLC data
        lows_list = open(f"lows_of_{symbol}.txt", 'a')
        lows_list.write(f' {low},')
        lows_list.close()
        highs_list = open(f"highs_of_{symbol}.txt", 'a')
        highs_list.write(f' {high},')
        highs_list.close()

daily_ohlc_data()
