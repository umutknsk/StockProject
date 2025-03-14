import pandas as pd
import os
import random
import psycopg2
from datetime import timedelta
from psycopg2 import sql

# Directory path containing the CSV files
directory_path = r'/home/umutknsk/stock-env/201805_1hour'

# Find all CSV files in the specified directory
csv_files = [os.path.join(directory_path, f) for f in os.listdir(directory_path) if f.endswith('.csv')]

# Dictionary to store randomly selected dates
random_dates_dict = {}

# Function to process each row of stock data
def process_row(row):
    timestamp = row['ts_event'] if pd.notna(row['ts_event']) else None  
    rtype = row['rtype'] if pd.notna(row['rtype']) else None  
    publisher_id = row['publisher_id'] if pd.notna(row['publisher_id']) else None  
    instrument_id = row['instrument_id'] if pd.notna(row['instrument_id']) else None  
    open_price = row['open'] if pd.notna(row['open']) else None  
    high_price = row['high'] if pd.notna(row['high']) else None  
    low_price = row['low'] if pd.notna(row['low']) else None  
    close_price = row['close'] if pd.notna(row['close']) else None  
    volume = row['volume'] if pd.notna(row['volume']) else None  
    symbol = row['symbol'] if pd.notna(row['symbol']) else None  

    return (timestamp, rtype, publisher_id, instrument_id, open_price, high_price, low_price, close_price, volume, symbol)

# Connect to PostgreSQL database
def connect_to_db():
    try:
        conn = psycopg2.connect(
            host="localhost",  
            database="daily_stock_data",  
            user="umutknsk",  
            password="umut1234"  
        )
        cur = conn.cursor()
        return conn, cur
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None, None

# SQL query to insert data into the database
def get_insert_query():
    return """
        INSERT INTO daily_stock_table (timestamp, rtype, publisher_id, instrument_id, open_price, high_price, low_price, close_price, volume, symbol)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (symbol, timestamp) DO NOTHING;
    """

# Function to randomly remove data from specific columns
def remove_data_randomly(df):
    # Get random rows to modify (100 random rows)
    random_rows = random.sample(range(df.shape[0]), 100)

    for idx in random_rows:
        # Randomly choose between 1 and 5 columns to nullify
        num_columns_to_nan = random.randint(1, 5)
        columns_to_nan = random.sample(["open", "high", "low", "close", "volume"], num_columns_to_nan)

        # Set the selected columns to NaN
        df.loc[idx, columns_to_nan] = None
    
    return df, [df.loc[idx, 'ts_event'] for idx in random_rows]  # Return modified rows and the timestamps of those rows

# Function to insert stock data into the database
def insert_stock_data():
    conn, cur = connect_to_db()
    if conn is None or cur is None:
        return

    insert_query = get_insert_query()

    if not csv_files:
        print("No CSV files found in the specified directory.")
        return

    for file_path in csv_files:
        try:
            df = pd.read_csv(file_path)
            table_name = os.path.basename(file_path).replace(".csv", "")

            if df.empty:
                print(f"No valid data to insert for {table_name}.")
                continue  # Skip empty files
            
            # Randomly remove data from the rows and get the removed dates
            df, removed_dates = remove_data_randomly(df)

            # Save the removed dates to the dictionary for later use
            random_dates_dict[table_name] = removed_dates

            # Prepare rows for database insertion
            rows_to_insert = df.apply(process_row, axis=1).tolist()

            # Insert data into the database
            cur.executemany(insert_query, rows_to_insert)
            conn.commit()  # Commit the transaction after inserting each file
            
            print(f"Data inserted successfully from {table_name}.")
        
        except Exception as e:
            print(f"Error processing file {file_path}: {e}")

    # Close database connection after processing all files
    cur.close()
    conn.close()

# Function to print the dates with removed data
def print_removed_dates():
    for table_name, removed_dates in random_dates_dict.items():
        print(f"Removed dates for {table_name}: {removed_dates}")

# Calling functions
insert_stock_data()
print_removed_dates()