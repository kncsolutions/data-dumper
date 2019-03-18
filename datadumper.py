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
import pandas as pd
from apscheduler.schedulers.blocking import BlockingScheduler

client = GfeedClient(sys.argv[1], sys.argv[2])
bs_client=None
fnostocksarray = ["SBIN","INFY","BEPL","HDIL","BPCL"];
data_sbin={"SBIN":0}
data_infy={"INFY":0}
data_bepl={"BEPL":0}
data_hdil={"HDIL":0}
data_bpcl={"BPCL":0}
data={"SBIN":0,"INFY":0,"BEPL":0,"HDIL":0,"BPCL":0}

exchange = 'NSE'
header=[]
headerWritten=False
prev_write_time=int(time.time()) 
current_time=int(time.time()) 
def run_threaded(job_func):
    job_thread = threading.Thread(target=job_func)
    job_thread.start()


def on_authenticated(base_client):
	global fnostocksarray
	global exchange
	global header
	global headerWritten
	global bs_client
	global data
	bs_client=base_client
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
	t1 = threading.Thread(target=task1, name='t1') 
	t2 = threading.Thread(target=task2, name='t2') 
	#for item in fnostocksarray:
		#historicaldatafinder(base_client,item)
		#schedule.every(10).seconds.do(run_threaded, historicaldatafinder(base_client,item))
	#schedule.every(1).minutes.do(run_threaded,write_data())
	# starting threads 
	t1.start() 
	t2.start()
	
	#t1.join() 
	#t2.join() 
	
def task1():
	global bs_client
	scheduler = BlockingScheduler()
	scheduler.add_job(get_data, 'interval', seconds=5)
	scheduler.start()
	for item in fnostocksarray:
		historicaldatafinder(bs_client,item)
	
 
def get_data():
	for item in fnostocksarray:
		historicaldatafinder(bs_client,item)
 
def task2():
	scheduler = BlockingScheduler()
	scheduler.add_job(write_data, 'interval', seconds=1)
	scheduler.start()
	
def write_data():
	global fnostocksarray
	global prev_write_time
	global current_time
	current_time=int(time.time())	
	if current_time-prev_write_time>=1:
		print('Updating File..............')
		line=[]
		tm=str(datetime.datetime.now().replace(microsecond=0))
		line.append(tm)
		#Here you update the file
		for i in range(1,len(header)):
			line.append(data[fnostocksarray[i-1]])
		dta = pd.read_csv("NSE.csv")
		tms=dta['LastTradeTime'].tolist()
		if tm not in tms:
			with open(exchange+'.csv', 'a',newline='') as csvFile:
				writer = csv.writer(csvFile)
				writer.writerow(line)
			prev_write_time=current_time
			csvFile.close()



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
	last_data=historical_ohlc_data['Result'].pop()
	data[historical_ohlc_data['Request']['InstrumentIdentifier']]=last_data['Close']
	print(historical_ohlc_data['Result'].pop())  
    

def on_message_snapshot_data(base_client,snapshot_data):
    print("\n*********SNAPSHOT DATA*************\n")
    print(snapshot_data)



client.on_authenticated = on_authenticated
client.on_message_historical_ohlc_data = on_message_historical_ohlc_data
client.on_message_snapshot_data = on_message_snapshot_data


client.connect()
