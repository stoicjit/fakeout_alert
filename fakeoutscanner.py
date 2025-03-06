from asyncio import sleep
import psycopg2
from tradingview_ta import TA_Handler, Interval
import time
import asyncio
from telegram import Bot
import os

# Telegram Bot Credentials
bot_token = os.getenv('BOT_TOKEN')  # Replace with your Telegram bot token
CHAT_ID = os.getenv('CHAT_ID')  # Replace with your Telegram chat ID
bot = Bot(token=bot_token)


# PostgreSQL connection
DB_URL = "postgresql://postgres:IBlWSdKzrIVmbcpiiSKoLXHDhRdOZuwj@metro.proxy.rlwy.net:44305/railway"  # Replace with your Railway PostgreSQL URL
conn = psycopg2.connect(DB_URL)
cursor = conn.cursor()

# Define symbols
symbols = ["BTCUSD", "ETHUSD", "LTCUSD","XRPUSD", "DOGEUSD","DOTUSD","ADAUSD"]
exchange = "COINBASE"
directions = ['high','low']

# Create a table if it doesn’t exist
def create_ohlc_table(symbol,direction):
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {direction}_data{symbol} (
            id SERIAL PRIMARY KEY,
            symbol TEXT,
            {direction} FLOAT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()

def store_daily_data(symbol, direction):
            ta = TA_Handler(
                symbol=symbol,
                exchange=exchange,
                screener="crypto",
                interval=Interval.INTERVAL_1_DAY,
            )
            analysis = ta.get_analysis()
            level = analysis.indicators.get(f"{direction}", None)
            print(f"{symbol} - High: ")

            # Insert into PostgreSQL
            cursor.execute(f"INSERT INTO {direction}_data{symbol} (symbol, {direction}) VALUES (%s, %s)",
                           (symbol, level))

            conn.commit()

            time.sleep(2)  # Prevent rate-limiting

def filter_highs(symbol):
    #Read data
    cursor.execute(f"SELECT * FROM high_data{symbol}")
    rows = cursor.fetchall()
    #Keep the significant highs
    for row in rows[0:-1]:
        print(f"symbol: {symbol}, row: {row}, price: {row[2]}, last price: {rows[-1][2]}")
        if row[2] <= rows[-1][2]:
            row_id = row[0]
            cursor.execute(f"DELETE FROM high_data{symbol} WHERE id = %s", (row_id,))
            conn.commit()
            print(f"Deleted row with ID {row_id}!")
            
def filter_lows(symbol):
    #Read data
    cursor.execute(f"SELECT * FROM low_data{symbol}")
    rows = cursor.fetchall()
    #Keep the significant levels
    for row in rows[0:-1]:
        if row[2] >= rows[-1][2]:
            row_id = row[0]
            cursor.execute(f"DELETE FROM low_data{symbol} WHERE id = %s", (row_id,))
            conn.commit()
            print(f"Deleted row with ID {row_id}!")


def compare(high, low, close):
    for symbol in symbols:
        for direction in directions:
            cursor.execute(f"SELECT * FROM {direction}_data{symbol}")
            rows = cursor.fetchall()
            for row in rows:
                if high > row[2] and close < row[2]:
                    async def send_telegram_message():
                        message = f'{symbol} just fakedout a {direction}'
                        await bot.send_message(chat_id=CHAT_ID, text=message)
                    asyncio.run(send_telegram_message())
                elif low < row[2] and close > row[2]:
                    async def send_telegram_message():
                        message = f'{symbol} just fakedout a {direction}'
                        await bot.send_message(chat_id=CHAT_ID, text=message)
                    asyncio.run(send_telegram_message())


def h_ohlc(symbol):
    ta = TA_Handler(
        symbol=symbol,
        exchange=exchange,
        screener="crypto",
        interval=Interval.INTERVAL_1_HOUR,
    )
    analysis = ta.get_analysis()
    high = analysis.indicators.get("high", None)
    low = analysis.indicators.get("low", None)
    close = analysis.indicators.get("close", None)
    time.sleep(3)
    return high, low, close





if time.localtime()[3] == 18:
    for symbol in symbols:
        for direction in directions:
            create_ohlc_table(symbol,direction)
            store_daily_data(symbol,direction)
        filter_highs(symbol)
        filter_lows(symbol)
for symbol in symbols:            
    high, low, close = h_ohlc(symbol)
    compare(high, low, close)

# Close connection
cursor.close()
conn.close()
