#!/usr/bin/python3
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

# Define MySQL connection variables
mysql_host = 'localhost'
mysql_user = 'root'
mysql_password = 'passw0rd'
database_name = 'dapnet_freepbx'

# URL to fetch DAPNET callsign data
dapnet_url = 'http://www.hampager.de:8080/callsigns'

# URL to download the RadioID CSV file
radioid_url = 'https://radioid.net/static/user.csv'

def connect_mysql():
    """Establishes a connection to MySQL and returns the connection object."""
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
    """Fetches DAPNET callsign data from the specified URL."""
    try:
        response = requests.get(url, auth=HTTPBasicAuth("n8acl", "Xyke8c11qD6I9vTpz63U"))
        response.raise_for_status()  # Raise an exception for HTTP errors
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching DAPNET callsign data: {e}")
        return None

def load_dapnet_callsigns(conn, dapnet_callsigns):
    """Loads DAPNET callsign data into MySQL database."""
    try:
        print("DAPNET data import started")
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS dapnet_data;")
        cursor.execute("CREATE TABLE dapnet_data (callsign TEXT);")
        for callsign_data in dapnet_callsigns:
            callsign = callsign_data.get('name', '')
            sql = f"INSERT INTO dapnet_data (callsign) VALUES ('{callsign}');"
            cursor.execute(sql)
        conn.commit()
        print("DAPNET data imported into MySQL successfully")
    except Error as e:
        print(f"Error loading DAPNET callsign data: {e}")

def import_radio_id(conn, csv_url):
    """Downloads CSV from URL and imports it into MySQL."""
    try:
        # Download CSV
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
            for index, row in df.iterrows():
                cursor.execute("INSERT INTO radioid_data (CALLSIGN, RADIO_ID) VALUES (%s, %s);", (row['CALLSIGN'], row['RADIO_ID']))
            conn.commit()
            print("RadioID data imported into MySQL successfully")
        else:
            print("Error: 'callsign' column not found in CSV data")
    except Exception as e:
        print(f"Error importing RadioID data into MySQL: {e}")

def main():
    now = datetime.now()
    current_time = now.strftime("%H:%M")
    print("Starting process at:", current_time)

    # Connect to MySQL
    conn = connect_mysql()
    if not conn:
        return

    # Load DAPNET callsign data
    dapnet_callsign_data = fetch_dapnet_callsigns(dapnet_url)
    if dapnet_callsign_data:
        load_dapnet_callsigns(conn, dapnet_callsign_data)

    # Import RadioID data
    import_radio_id(conn, radioid_url)

    # Clean up RadioID data
#    try:
#        cursor = conn.cursor()
#        cursor.execute("DELETE FROM radioid_data WHERE CALLSIGN NOT IN (SELECT UPPER(callsign) AS callsign FROM dapnet_data)")
#        conn.commit()
#        print("RadioID data cleaned up successfully")
#    except Error as e:
#        print(f"Error cleaning up RadioID data: {e}")

#    conn.close()
    print("Process completed at:", datetime.now().strftime("%H:%M"))

if __name__ == "__main__":
    main()
