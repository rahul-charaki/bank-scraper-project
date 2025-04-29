# Importing the required libraries
import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import numpy as np
from datetime import datetime

def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the code execution to a log file. Function returns nothing'''
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open("code_log.txt", "a") as f:
        f.write(f"{timestamp} : {message}\n")

def extract(url, table_attribs):
    ''' This function aims to extract the required information from the website and save it to a data frame. 
        The function returns the data frame for further processing. '''
    page = requests.get(url).text
    soup = BeautifulSoup(page, "html.parser")
    tables = soup.find_all('table', {'class':'wikitable'})
    df = pd.read_html(str(tables[0]))[0]  # First table is required

    df = df[[table_attribs[0], table_attribs[1]]]
    
    # Remove unwanted characters and convert Market Cap to float
    df[table_attribs[1]] = df[table_attribs[1]].replace('[\n]', '', regex=True).astype(float)
    
    return df

def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate information,
        and adds three columns to the data frame, each containing the 
        transformed version of Market Cap column to respective currencies '''
    exchange_rate = pd.read_csv(csv_path)
    exchange_rate_dict = dict(zip(exchange_rate['Currency'], exchange_rate['Rate']))
    
    df['MC_GBP_Billion'] = [np.round(x * exchange_rate_dict['GBP'], 2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x * exchange_rate_dict['EUR'], 2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x * exchange_rate_dict['INR'], 2) for x in df['MC_USD_Billion']]
    
    return df

def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in the provided path. Function returns nothing. '''
    df.to_csv(output_path, index=False)

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database table with the provided name. Function returns nothing. '''
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and prints the output on the terminal. Function returns nothing. '''
    cursor = sql_connection.cursor()
    cursor.execute(query_statement)
    results = cursor.fetchall()
    for row in results:
        print(row)

# ------------------- MAIN EXECUTION AREA -------------------

# Declaring known values
url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
csv_path = './exchange_rate.csv'
table_attribs = ['Name', 'MC_USD_Billion']
output_csv_path = './Largest_banks_data.csv'
db_name = 'Banks.db'
table_name = 'Largest_banks'

log_progress("Preliminaries complete. Initiating ETL process")

# Extraction
df = extract(url, table_attribs)
log_progress("Data extraction complete. Initiating Transformation process")

# Transformation
df = transform(df, csv_path)
log_progress("Data transformation complete. Initiating Loading process")

# Loading to CSV
load_to_csv(df, output_csv_path)
log_progress("Data saved to CSV file")

# Loading to DB
sql_connection = sqlite3.connect(db_name)
log_progress("SQL Connection initiated")

load_to_db(df, sql_connection, table_name)
log_progress("Data loaded to Database as a table, Executing queries")

# Running queries
print("\nAll Banks:")
run_query(f"SELECT * FROM {table_name}", sql_connection)

print("\nTop 5 Banks by USD Market Cap:")
run_query(f"SELECT Name, MC_USD_Billion FROM {table_name} ORDER BY MC_USD_Billion DESC LIMIT 5", sql_connection)

print("\nBanks with Market Cap over 300 Billion USD:")
run_query(f"SELECT Name FROM {table_name} WHERE MC_USD_Billion > 300", sql_connection)

log_progress("Process Complete")

# Closing connection
sql_connection.close()
log_progress("Server Connection closed")
