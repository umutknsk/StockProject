# Import necessary libraries
import yfinance as yf
import psycopg2
from psycopg2 import sql
import datetime
import holidays
from datetime import datetime, timedelta
import random
import pandas as pd

# Define stock symbols and holidays
symbols = ['AAPL', 'NVDA', 'SOUN', 'SMCI']

# Dictionary to store randomly selected dates
random_dates_dict = {symbol: [] for symbol in symbols}

# Function to process each row of stock data
def process_row(row, symbol):
    name = row.get('shortName', 'Unknown Name')  
    open_price = row['Open'] if pd.notna(row['Open']) else None  
    high = row['High'] if pd.notna(row['High']) else None  
    low = row['Low'] if pd.notna(row['Low']) else None  
    close = row['Close'] if pd.notna(row['Close']) else None 
    volume = row['Volume'] if pd.notna(row['Volume']) else None  
    dividends = row['Dividends'] if pd.notna(row['Dividends']) else None 
    stock_splits = row['Stock Splits'] if pd.notna(row['Stock Splits']) else None  
    timestamp = row.name  
    
    return (symbol, name, open_price, high, low, close, volume, dividends, stock_splits, timestamp)

# Connect to PostgreSQL database
try:
    conn = psycopg2.connect(
        host="localhost",  
        database="stock_data",  
        user="umutknsk",  
        password="umut1234"  
    )
    cur = conn.cursor()

    # SQL query to insert data into the database
    insert_query = sql.SQL("""
        INSERT INTO stock_table (symbol, name, open, high, low, close, volume, dividends, stock_splits, timestamp)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (symbol, timestamp) DO NOTHING;
    """)
        
    # Loop through each symbol and retrieve stock data
    for symbol in symbols:
        stock = yf.Ticker(symbol)
        stock_info = stock.history(period='max')  # Fetch historical data for the stock

        if stock_info.empty:
            print(f"No stock data available for {symbol}.")  # Handle empty data
            continue

        stock_info.index = stock_info.index.tz_localize(None)  # Remove timezone info

        first_date = stock_info.index.min()  # Get the first date in the data
        last_date = stock_info.index.max()  # Get the last date in the data
        start_date = first_date.replace(tzinfo=None)  # Adjust start date to remove timezone

        print(f"First date is {first_date} for {symbol}")

        delta_days = (last_date - start_date).days  # Calculate the number of days between first and last date
        if delta_days <= 0:
            continue

        us_holidays = holidays.US(years=range(first_date.year, last_date.year + 1))  # Get US holidays for the year range

        fully_removed_dates = set()  # Set to store fully removed dates
        
        # Randomly select 10 dates to fully remove (exclude weekends and holidays)
        while len(fully_removed_dates) < min(10, delta_days):  
            random_days = random.randint(0, delta_days)
            random_date = start_date + timedelta(days=random_days)

            if random_date.weekday() < 5 and random_date.date() not in us_holidays:
                fully_removed_dates.add(random_date.date())

        random_dates_dict[symbol] = list(fully_removed_dates)  # Store the fully removed dates

        print(f"Selected dates (excluded) for {symbol}: {random_dates_dict[symbol]}")

        partially_removed_dates = set()  # Set to store partially removed dates
        
        # Randomly select 10 dates to partially remove (exclude weekends and holidays)
        while len(partially_removed_dates) < min(10, delta_days):  
            random_days = random.randint(0, delta_days)
            random_date = start_date + timedelta(days=random_days)

            if (random_date.weekday() < 5 and random_date.date() not in us_holidays and 
                random_date.date() not in fully_removed_dates):
                partially_removed_dates.add(random_date.date())
        
        print(f"Partially removed dates for {symbol}: {list(partially_removed_dates)}")

        partially_removed_dates = pd.to_datetime(list(partially_removed_dates))  # Convert to pandas datetime format

        # Update stock data by setting random columns to NaN on partially removed dates
        for date in partially_removed_dates:
            if date in stock_info.index:
                num_columns_to_nan = random.randint(1, 4)  # Randomly select number of columns to set NaN
                columns_to_nan = random.sample(["Open", "High", "Low", "Close", "Volume"], num_columns_to_nan)
                stock_info.loc[stock_info.index == date, columns_to_nan] = None
            
        print(f"Updated stock data for {symbol}:\n", stock_info.loc[stock_info.index.isin(partially_removed_dates)])
        print("\n")
        
        # Detect partially missing data (dates with NaN values in stock data)
        partially_missing_dates = stock_info[stock_info.isna().any(axis=1)].index.date.tolist()
        print(f"For {symbol}, partially missing dates (with NaN values) are: {partially_missing_dates}")
        print("\n")
        
        # Filter stock data by excluding fully removed dates
        stock_info['Date'] = stock_info.index.date
        stock_info_filtered = stock_info[~stock_info['Date'].isin(random_dates_dict[symbol])]

        if stock_info_filtered.empty:
            print(f"No valid data to insert for {symbol}.")  # Handle case with no valid data
            continue

        # Prepare rows for database insertion
        rows_to_insert = stock_info_filtered.apply(lambda row: process_row(row, symbol), axis=1).tolist()
        cur.executemany(insert_query, rows_to_insert)  # Insert the rows into the database
    
    # Commit the transaction after inserting all data
    conn.commit()
    
    # Loop again to fetch and analyze missing dates from the database
    for symbol in symbols:
        stock = yf.Ticker(symbol)
        stock_info = stock.history(period='max')  # Fetch stock data again

        if stock_info.empty:
            print(f"No stock data available for {symbol}.")  # Handle empty data
            continue

        stock_info.index = stock_info.index.tz_localize(None)  # Remove timezone info

        first_date = stock_info.index.min()  # Get the first date in the data
        last_date = stock_info.index.max()  # Get the last date in the data
        start_date = first_date.replace(tzinfo=None)  # Adjust start date to remove timezone

        delta_days = (last_date - first_date).days  # Calculate the number of days between first and last date
        if delta_days <= 0:
            continue
        
        # Get holidays for the years of the data
        us_holidays = holidays.US(years=range(first_date.year, last_date.year + 1))
        
        # List all valid trading days excluding weekends and holidays
        all_trading_days = [i.date() for i in pd.date_range(start=start_date, end=last_date) 
                            if i.weekday() < 5 and i.date() not in us_holidays]
        
        # Get existing dates from the database for the symbol
        cur.execute("""SELECT DISTINCT timestamp FROM stock_table WHERE symbol = %s;""", (symbol,))
        existing_dates = {row[0].date() for row in cur.fetchall()}
        
        # Get missing dates by excluding existing dates
        missing_dates = sorted(set(all_trading_days) - existing_dates)

        # Remove holidays from missing dates (make sure holidays are excluded from the final list)
        missing_dates = [date for date in missing_dates if date not in us_holidays]

        print(f"For {symbol}, missing dates are: {missing_dates}")
        print("\n")
        
    # Close the cursor and connection
    cur.close()
    conn.close()

    print("All data inserted successfully!")

# Handle any exceptions that occur during the process
except Exception as e:
    print(f"Error occurred: {e}")