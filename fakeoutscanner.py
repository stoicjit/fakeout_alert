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
DB_URL = os.getenv('DB_URL')  # Replace with your Railway PostgreSQL URL
conn = psycopg2.connect(DB_URL)
cursor = conn.cursor()

# Define symbols
symbols = ["BTCUSD", "ETHUSD", "LTCUSD", "XRPUSD", "DOGEUSD", "DOTUSD", "ADAUSD"]
exchange = "COINBASE"
directions = ['high', 'low']


# Create a table if it doesn’t exist
# def create_ohlc_table(symbol, direction):
#     cursor.execute(f"""
#         CREATE TABLE IF NOT EXISTS {direction}_data{symbol} (
#             id SERIAL PRIMARY KEY,
#             symbol TEXT,
#             {direction} FLOAT,
#             timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
#         )
#     """)
#     conn.commit()


def store_daily_data(symbol, direction):
    try:
        ta = TA_Handler(
            symbol=symbol,
            exchange=exchange,
            screener="crypto",
            interval=Interval.INTERVAL_1_DAY,
        )
        analysis = ta.get_analysis()
        level = analysis.indicators.get(f"{direction}", None)
        print(f"{symbol} - {direction}: {level}")
    
        # Insert into PostgreSQL
        cursor.execute(f"INSERT INTO {direction}_data{symbol} (symbol, {direction}) VALUES (%s, %s)",
                       (symbol, level))
    
        conn.commit()
        
    except Exception as e:
        print(f"❌ Error storing {level} for {symbol} - {direction}: {e}")

    finally:
        time.sleep(1)  # Prevent rate-limiting


def filter_highs(symbol):
    # Read data
    cursor.execute(f"SELECT * FROM high_data{symbol} ORDER BY timestamp ASC")
    rows = cursor.fetchall()
    # Keep the significant highs
    for row in rows[0:-1]:
        print(f"symbol: {symbol}, row: {row}, price: {row[2]}, last price: {rows[-1][2]}")
        if row[2] <= rows[-1][2]:
            row_id = row[0]
            cursor.execute(f"DELETE FROM high_data{symbol} WHERE id = %s", (row_id,))
            conn.commit()
            print(f"Deleted row with ID {row_id}!")


def filter_lows(symbol):
    # Read data
    cursor.execute(f"SELECT * FROM low_data{symbol} ORDER BY timestamp ASC")
    rows = cursor.fetchall()
    # Keep the significant levels
    for row in rows[0:-1]:
        if row[2] >= rows[-1][2]:
            row_id = row[0]
            price = row[2]
            cursor.execute(f"DELETE FROM low_data{symbol} WHERE id = %s", (row_id,))
            conn.commit()
            print(f"Deleted row {row_id}; Price: {price}")


async def send_telegram_message(message):
    """Sends an async Telegram message."""
    await bot.send_message(chat_id=CHAT_ID, text=message)


async def compare_highs(symbol, high, close):
    cursor.execute(f"SELECT * FROM high_data{symbol} ORDER BY timestamp ASC")
    rows = cursor.fetchall()
    tasks = []

    for row in rows[-7:]:
        print(row[2])
        if high > row[2] and close < row[2]:
            print(f'{symbol} faked out the daily high')
            message = f'{symbol} just faked out a daily high'
            tasks.append(send_telegram_message(message))  # Collect tasks

    await asyncio.gather(*tasks)  # Run all tasks concurrently


async def compare_lows(symbol, low, close):
    cursor.execute(f"SELECT * FROM low_data{symbol} ORDER BY timestamp ASC")
    rows = cursor.fetchall()
    tasks = []

    for row in rows[-7:]:
        print(row[2])
        if low < row[2] and close > row[2]:
            print(f'{symbol} faked out the daily low')
            message = f'{symbol} just faked out a daily low'
            tasks.append(send_telegram_message(message))  # Collect tasks

    await asyncio.gather(*tasks)  # Run all tasks concurrently


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
    time.sleep(1)
    return high, low, close


async def main():
    if time.localtime()[3] == 4: #23
        for symbol in symbols:
            for direction in directions:
                store_daily_data(symbol, direction)
                print(f"The {direction} of {symbol} has been stored")
            filter_highs(symbol)
            print(f"The highs of {symbol}: filtered")
            filter_lows(symbol)
            print(f"The lows of {symbol}: filtered")

    tasks = []  # Collect async tasks
    for symbol in symbols:
        high, low, close = h_ohlc(symbol)
        print(symbol, high, low, close)
        tasks.append(compare_highs(symbol, high, close))
        tasks.append(compare_lows(symbol, low, close))

    await asyncio.gather(*tasks)  # Run all Telegram messages concurrently

asyncio.run(main())  # Call the main function properly

# Close connection
cursor.close()
conn.close()
