#!/usr/bin/python3
from requests.auth import HTTPBasicAuth
import http.client
import json
import mysql.connector
import os
import requests
import sys

#############################
##### Define Variables

# define mysql connection variables
mysql_host = 'localhost'
mysql_user = 'root'
mysql_password = 'passw0rd'
mysql_databasename = 'dapnet_freepbx'  # DO NOT CHANGE THIS

#############################
##### DO NOT CHANGE BELOW

linefeed = "\r\n"
dapnet_url = 'http://www.hampager.de:8080/calls'
dapnet_data = os.path.dirname(os.path.abspath(__file__)) + "/dapnet.json"

# configure Database connection for MySQL Connector
db_config = {
    'host': mysql_host,
    'database': mysql_databasename,
    'user': mysql_user,
    'password': mysql_password,
    'auth_plugin': 'mysql_native_password'
}

# Load DAPNET Credentials
with open(dapnet_data) as df:
    creds_data = json.load(df)

#############################
##### Define Functions

def select_sql(conn, sql):
    # Executes SQL for Selects - Returns a "value"
    try:
        cur = conn.cursor()
        cur.execute(sql)
        return cur.fetchall()
    except mysql.connector.Error as e:
        print("MySQL Error:", e)
        sys.exit(1)

def send_dapnet(creds, send_to, text):
    data = json.dumps({"text": text, "callSignNames": [send_to], "transmitterGroupNames": [creds["tx_group"]],
                       "emergency": False})
    try:
        response = requests.post(dapnet_url, data=data, auth=HTTPBasicAuth(creds["username"], creds["password"]))
        if response.status_code != 200:
            print("DAPNET Error:", response.text)
            sys.exit(1)
    except Exception as e:
        print("DAPNET Request Error:", e)
        sys.exit(1)

#############################
##### Main Program

# Create a MySQL connection
try:
    conn = mysql.connector.connect(**db_config)
except mysql.connector.Error as e:
    print("MySQL Connection Error:", e)
    sys.exit(1)

# Load parameters from command line arguments
if len(sys.argv) != 4:
    print("Usage: python3 script.py <send_to_ric> <callback> <calling_from>")
    sys.exit(1)

send_to_ric = sys.argv[1]
callback = sys.argv[2]
calling_from = sys.argv[3]

# Get the callsign from the extension calling from
sql = "SELECT callsign FROM ext_data WHERE extension = '{}'".format(calling_from)
results = select_sql(conn, sql)

if not results:
    print("Error: Unable to find callsign for the provided extension:", calling_from)
    sys.exit(1)

from_callsign = results[0][0]  # Extracting the callsign from the first row of the result

# Get Callsign to send to from DMR ID
sql = "SELECT callsign FROM radioid_data WHERE radio_id = {}".format(send_to_ric)
results = select_sql(conn, sql)

if not results:
    print("Error: Unable to find callsign for the provided DMR ID:", send_to_ric)
    sys.exit(1)

to_callsign = results[0][0]  # Extracting the callsign from the first row of the result

# Build Message and then send
send_dapnet(creds_data["my_creds"], to_callsign, "{}: Call me at {} #HAMVOIP Dapnet Gateway".format(from_callsign, callback))
