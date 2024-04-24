#!/usr/bin/python3
import argparse  
import json
import pandas as pd
import requests  
import os  
from requests.auth import HTTPBasicAuth  
import mysql.connector
from mysql.connector import Error
import sqlalchemy
from datetime import datetime
import io
import csv
import time  

# Define MySQL connection variables
mysql_host = 'localhost'
mysql_user = 'root'
mysql_password = 'passw0rd'
database_name = 'dapnet_freepbx'

# URL to fetch DAPNET callsign data
dapnet_url = 'http://www.hampager.de:8080/callsigns'

# URL to download the RadioID CSV file
radioid_url = 'https://radioid.net/static/user.csv'

# Define script version
script_version = '1.1'

def connect_mysql():
    # Establishes a connection to MySQL and returns the connection object.
    try:
        conn = mysql.connector.connect(
            host=mysql_host,
            user=mysql_user,
            password=mysql_password,
            database=database_name,
            auth_plugin='mysql_native_password'
        )
        print("Connected to MySQL database")
        return conn
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
        return None

def fetch_dapnet_callsigns(url):
    # Fetches DAPNET callsign data from the specified URL.
    try:
        print("DAPNET data download started")
        response = requests.get(url, auth=HTTPBasicAuth("n8acl", "Xyke8c11qD6I9vTpz63U"))
        response.raise_for_status()  # Raise an exception for HTTP errors
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching DAPNET callsign data: {e}")
        return None

def load_dapnet_callsigns(conn, dapnet_callsigns):
    # Loads DAPNET callsign data into MySQL database.
    try:
        print("DAPNET data import started")
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS dapnet_data;")
        cursor.execute("CREATE TABLE dapnet_data (callsign TEXT);")
        total_rows = len(dapnet_callsigns)
        current_row = 0
        for callsign_data in dapnet_callsigns:
            callsign = callsign_data.get('name', '')
            sql = f"INSERT INTO dapnet_data (callsign) VALUES ('{callsign}');"
            cursor.execute(sql)
            current_row += 1
            print(f"Imported {current_row}/{total_rows} rows into DAPNET database ({current_row / total_rows * 100:.2f}%)", end='\r')
        conn.commit()
        print("\nDAPNET data imported into MySQL successfully")
        return total_rows
    except Error as e:
        print(f"Error loading DAPNET callsign data: {e}")
        return 0

def import_radio_id(conn, csv_url):
    # Downloads CSV from URL and imports it into MySQL.
    try:
        # Download CSV
        print("RADIOID data download started")
        response = requests.get(csv_url)
        response.raise_for_status()  # Raise an exception for HTTP errors
        
        # Read CSV into DataFrame
        df = pd.read_csv(io.StringIO(response.text))
        
        # Check if 'callsign' column exists in the DataFrame
        if 'CALLSIGN' in df.columns:
            # Create new table and import data
            print("RADIOID data import started")
            cursor = conn.cursor()
            cursor.execute("DROP TABLE IF EXISTS radioid_data;")
            cursor.execute("CREATE TABLE radioid_data (CALLSIGN TEXT, RADIO_ID INT);")
            total_rows = len(df)
            current_row = 0
            for index, row in df.iterrows():
                cursor.execute("INSERT INTO radioid_data (CALLSIGN, RADIO_ID) VALUES (%s, %s);", (row['CALLSIGN'], row['RADIO_ID']))
                current_row += 1
                print(f"Imported {current_row}/{total_rows} rows into RadioID database ({current_row / total_rows * 100:.2f}%)", end='\r')
            conn.commit()
            print("\nRadioID data imported into MySQL successfully")
            return total_rows
        else:
            print("Error: 'callsign' column not found in CSV data")
            return 0
    except Exception as e:
        print(f"Error importing RadioID data into MySQL: {e}")
        return 0

def import_local_data(conn, csv_file):
    # Imports local CSV data into MySQL.
    try:
        # Read CSV into DataFrame
        df = pd.read_csv(csv_file)

        # Import data into MySQL
        print("Local data import started")
        with conn.cursor() as cursor:
            cursor.execute("DROP TABLE IF EXISTS ext_data;")
            cursor.execute("CREATE TABLE ext_data (callsign TEXT, extension TEXT);")
            total_rows = len(df)
            current_row = 0
            for index, row in df.iterrows():
                cursor.execute("INSERT INTO ext_data (callsign, extension) VALUES (%s, %s);", (row['callsign'], row['extension']))
                current_row += 1
                print(f"Imported {current_row}/{total_rows} rows into local database ({current_row / total_rows * 100:.2f}%)", end='\r')
            conn.commit()
        print("\nLocal data imported into MySQL successfully")
        return total_rows
    except Exception as e:
        print(f"Error importing local data into MySQL: {e}")
        return 0

class LocalAction(argparse.Action):
    # Custom action to handle the -l option
    def __call__(self, parser, namespace, values, option_string=None):
        if values is None:
            values = 'ext_data.csv'

        if not os.path.exists(values):
            print(f"Error: Local CSV file '{values}' not found.")
            make_sample_file = input("Do you want to create a sample file? (y/n): ")
            if make_sample_file.lower() in ['y', 'yes', '']:
                with open(values, 'w', newline='') as csvfile:
                    writer = csv.writer(csvfile)
                    writer.writerow(['callsign', 'extension'])
                    writer.writerow(['PD2EMC', '425'])
                print(f"Sample file '{values}' created.")
            else:
                print("Exiting script.")
                parser.exit()

        setattr(namespace, self.dest, values)

def main(script_version):
    start_time = time.time()  # Record the start time

    # Parse command-line arguments
    parser = argparse.ArgumentParser(description='Process DAPNET, RadioID, and local data.')
    parser.add_argument('-d', '--dapnet', action='store_true', help='Load DAPNET database into MySQL')
    parser.add_argument('-r', '--radioid', action='store_true', help='Load RadioID database into MySQL')
    parser.add_argument('-l', '--local', action=LocalAction, nargs='?', const='ext_data.csv', help='Import CSV data into MySQL')
    parser.add_argument('-v', '--version', action='version', version=script_version, help='Show version')

    args = parser.parse_args()

    # Check if any options are provided
    if not any(vars(args).values()):
        args.dapnet = True
        args.radioid = True
        args.local = 'ext_data.csv'

    # Print starting process time
    now = datetime.now()
    current_time = now.strftime("%H:%M")
    print("Starting process at:", current_time)

    # Connect to MySQL
    conn = connect_mysql()
    if not conn:
        return

    total_dapnet_rows = total_radio_id_rows = total_local_rows = 0

    # Import local data
    if args.local:
        total_local_rows = import_local_data(conn, args.local)

    # Load DAPNET callsign data
    if args.dapnet:
        dapnet_callsign_data = fetch_dapnet_callsigns(dapnet_url)
        if dapnet_callsign_data:
            total_dapnet_rows = load_dapnet_callsigns(conn, dapnet_callsign_data)

    # Import RadioID data
    if args.radioid:
        total_radio_id_rows = import_radio_id(conn, radioid_url)

    # Close MySQL connection
    conn.close()

    # Print completion time and elapsed time
    end_time = time.time()
    elapsed_time = end_time - start_time
    print("Process completed at:", datetime.now().strftime("%H:%M"))
    print("Time taken:", round(elapsed_time, 2), "seconds (", round(elapsed_time / 60, 2), "minutes)")

    # Display overview of imported rows
    print("\nOverview of imported rows:")
    print("DAPNET database:", total_dapnet_rows, "rows")
    print("RadioID database:", total_radio_id_rows, "rows")
    print("Local database:", total_local_rows, "rows")

if __name__ == "__main__":
    main(script_version)  # Pass script version to main function
