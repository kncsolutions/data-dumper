"""
Copyright [2019] [Pallav Nandi Chaudhuri]

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

"""
from DhelmGfeedClient.gfeedclient import GfeedClient
from DhelmGfeedClient.constants import Constants
import sys
import json
import schedule
import time
import datetime
import threading
import csv
import os.path
from os import path

client = GfeedClient(sys.argv[1], sys.argv[2])
fnostocksarray = ["SBIN","INFY","BEPL","HDIL","BPCL"];
exchange = 'NSE'
header=[]
headerWritten=False

def run_threaded(job_func):
    job_thread = threading.Thread(target=job_func)
    job_thread.start()


def on_authenticated(base_client):
    global fnostocksarray
    global exchange
    global header
    global headerWritten
    # Here you create the file
    if headerWritten==False:
        header.append('LastTradeTime')    
        for item in fnostocksarray:
            header.append(item+'_LTP')
        print(header)
        with open(exchange+'.csv', 'w',newline='') as csvfile:
            filewriter = csv.writer(csvfile, delimiter=',',quotechar='|', quoting=csv.QUOTE_MINIMAL)
            filewriter.writerow(header)
            csvfile.close()
        headerWritten=True
    
    for item in fnostocksarray:
        #historicaldatafinder(base_client,item)
        schedule.every(10).seconds.do(run_threaded, historicaldatafinder(base_client,item))
   

def historicaldatafinder(base_client,item):
    base_client.get_historical_ohlc_data("NSE", item, Constants.DAY,
                                int(time.mktime((datetime.datetime(2018, 12, 13)).timetuple())),
                                int(time.mktime((datetime.datetime.now()).timetuple())), 10)
    #getRealTime(base_client,item)

def getRealTime(base_client,item):
    base_client.subscribe_realtime_snapshot("NSE",item, Constants.MINUTE)

def on_message_historical_ohlc_data(base_client,historical_ohlc_data):
    print("\n*********HISTORICAL OHLC DATA*************\n")
    #print(historical_ohlc_data)
    idx=-1
    for item in fnostocksarray:
        if historical_ohlc_data['Request']['InstrumentIdentifier'] == item:
            idx=fnostocksarray.index(item)
            break
    print('Index : '+str(idx))
    line=[]
    data=historical_ohlc_data['Result'].pop()
    line.append(str(data['LastTradeTime']))
    #Here you update the file
    for i in range(1,len(header)):
        if historical_ohlc_data['Request']['InstrumentIdentifier'] not in header[i]:
            line.append('-')
        else:
            line.append(str(data['Close']))
    with open(exchange+'.csv', 'a',newline='') as csvFile:
        writer = csv.writer(csvFile)
        writer.writerow(line)
    csvFile.close()
    print(historical_ohlc_data['Result'].pop())  
    base_client.subscribe_realtime_snapshot("NSE",historical_ohlc_data['Request']['InstrumentIdentifier'], Constants.MINUTE)

def on_message_snapshot_data(base_client,snapshot_data):
    print("\n*********SNAPSHOT DATA*************\n")
    print(snapshot_data)



client.on_authenticated = on_authenticated
client.on_message_historical_ohlc_data = on_message_historical_ohlc_data
client.on_message_snapshot_data = on_message_snapshot_data


client.connect()
