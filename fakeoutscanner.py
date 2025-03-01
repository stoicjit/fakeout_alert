import psycopg2
from tradingview_ta import TA_Handler, Interval
import time

# PostgreSQL connection
DB_URL = "postgresql://postgres:IBlWSdKzrIVmbcpiiSKoLXHDhRdOZuwj@metro.proxy.rlwy.net:44305/railway"  # Replace with your Railway PostgreSQL URL
conn = psycopg2.connect(DB_URL)
cursor = conn.cursor()

# Define symbols
symbols = ["BTCUSD", "ETHUSD", "LTCUSD"]
exchange = "COINBASE"

# Create a table if it doesn’t exist
cursor.execute("""
    CREATE TABLE IF NOT EXISTS ohlc_data (
        id SERIAL PRIMARY KEY,
        symbol TEXT,
        high FLOAT,
        low FLOAT,
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
""")
conn.commit()

def store_ohlc_data():
    for symbol in symbols:
        try:
            ta = TA_Handler(
                symbol=symbol,
                exchange=exchange,
                screener="crypto",
                interval=Interval.INTERVAL_1_HOUR,
            )
            analysis = ta.get_analysis()
            high = analysis.indicators.get("high", None)
            low = analysis.indicators.get("low", None)

            print(f"{symbol} - High: {high}, Low: {low}")

            # Insert into PostgreSQL
            cursor.execute("INSERT INTO ohlc_data (symbol, high, low) VALUES (%s, %s, %s)", 
                           (symbol, high, low))
            conn.commit()

            time.sleep(2)  # Prevent rate-limiting

        except Exception as e:
            print(f"Error fetching data for {symbol}: {e}")

# Run the function
store_ohlc_data()

# Close connection
cursor.close()
conn.close()
