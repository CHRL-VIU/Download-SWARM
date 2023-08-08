#!/home/alex/.local/lib/python3.6/ # specify python installation on server
# -*- coding: utf-8 -*-
# version 1.0.0

# This code reads in the live SWARM satellite messages from Mt Maya Wx station, 
# checks the latest CHRL SQL entry on the 'raw' database and if the latest 
# record is not on the SQL database, it pushes the new data to 'raw_maya' SQL 
# database for VIU-Hydromet. 
# Written by J. Bodart

import base64
import requests
import pandas as pd 
import numpy as np
from datetime import datetime

# Establish a connection with MySQL database 'viuhydro_wx_data_v2'
# Server log-in details stored in config file
import config
engine = config.main_sql()
conn = engine.connect()

# download SWARM satellite data from the server
# define output of the REST request as json
# and other parameterized values used below
loginHeaders = {'Content-Type': 'application/x-www-form-urlencoded'}
hdrs = {'Accept': 'application/json'}
loginParams = config.main_swarm()

hiveBaseURL = 'https://bumblebee.hive.swarm.space/hive'
loginURL = hiveBaseURL + '/login'
getMessageURL = hiveBaseURL + '/api/v1/messages'

# create a session
with requests.Session() as s:
    # log in to get the JSESSIONID cookie
    res = s.post(loginURL, data=loginParams, headers=loginHeaders)

if res.status_code != 200:
    print("Invalid username or password; please use a valid username and password in loginParams.")
    exit(1)

# let the session manage the cookie and get the output for the given appID
# only pull the last 10 items that have not been ACK'd
res = s.get(getMessageURL, headers=hdrs, params={'count': 1000, 'status': 0})
messages = res.json()

# for all the items in the json returned (limited to 10 above)
msg = []
for item in messages:
    # if there is a 'data' keypair, output the data portion converting it from base64 to ascii - assumes not binary
    if (item['data']):
        #print(base64.b64decode(item['data']).decode('ascii') + '\n')
        msg.append(base64.b64decode(item['data']).decode('ascii'))

while True:
    # get message data into dataframe and clean to match standard output
    df_sat = pd.DataFrame([sub.split(",") for sub in msg[::-1]])
    
    # put datetime column together based on individual columns
    datetimes = df_sat[[2, 3, 4, 5]].astype(str).astype(np.int64)
    datetimes.columns = ["year","month","day","hours"]
    dt = pd.to_datetime(datetimes)
    
    # remove July 13 2023 from database as it is erroneous and reset indices
    idx_err = [i for i in range(len(dt)) if '2023-07-13' in str(dt[i])]
    df_sat = pd.DataFrame.drop(df_sat, idx_err)
    dt = pd.Series.drop(dt, idx_err)
    df_sat = pd.Series.reset_index(df_sat,drop=True)
    dt = pd.Series.reset_index(dt,drop=True)
    
    # make sure you sort messages from older to newer dates as satellite sometimes 
    # sends multiple records at same time which are not sorted from older to newer
    dt = pd.Series(sorted(dt, key=lambda x: (x, datetime)))
    
    # read existing SQL entry with data and check if new data needs writing
    print('Checking for new data from satellite')      
    sql_file = pd.read_sql(sql="SELECT * FROM raw_mountmaya", con = engine)
    last_dt_sql = sql_file['DateTime'].iloc[-1]
    last_dt_system = dt.iloc[-1]
    
    # if the last row in SWARM matches last row in SQL database (i.e. no new data to
    # write), then exit and don't write new data to databse
    check = last_dt_sql == last_dt_system    
    if check:
        print('No new data detected - check satellite transmission?')  
        
        # write current time for sanity check and exit loop
        current_dateTime = datetime.now()
        print("Done at:", current_dateTime, '- refreshing in 1 hour...')
        break

    # else if new data on system which is not yet written to SQL, write it
    else:
        print('New satellite data detected - writing to SQL database') 
        
        # first find missing indices in SQL database
        if last_dt_sql < dt.iloc[0]:
            # safeguard in case the latest data on SQL is before the satelite 
            # record started (should only happen when satelite connection 
            # established for first time)
            last_idx = len(df_sat)
        else:
            # else calculate latest SQL entry and assess how many new 
            # satellite data to write to SQL database
            last_dt_sql_idx = int(np.flatnonzero(last_dt_sql == dt)[0])
            last_idx = df_sat.index[-1] - last_dt_sql_idx
            
        # only keep new data that needs added to sql database
        missing_data_df = df_sat.iloc[-last_idx:]
        
        # recalculate dt for missing data
        datetimes = missing_data_df[[2, 3, 4, 5]].astype(str).astype(np.int64)
        datetimes.columns = ["year","month","day","hours"]
        dt = pd.to_datetime(datetimes)
        
        # export new data to last row of SQL database  
        # No data values will automatically be added in SQL database as 
        # 'NULL'
        new_row = pd.DataFrame({'DateTime':dt,
                   'BattV_Avg':missing_data_df[6].astype(float),
                   'AirTC_Avg':missing_data_df[7].astype(float),
                   'RH_Avg':missing_data_df[8].astype(float),
                   'TCDT_Avg':missing_data_df[9].astype(float),
                   'WS_ms_Avg':missing_data_df[10].astype(float),
                   'WS_ms_Max':missing_data_df[11].astype(float),
                   'WindDir_D1_WVT':missing_data_df[12].astype(float),
                   'WindDir_SD1_WVT':missing_data_df[13].astype(float),
                   'Rain_mm_Tot':missing_data_df[14].astype(float),
                   'BaroP_Avg':missing_data_df[15].astype(float),
                   'SolarRad_Avg':missing_data_df[16].astype(float),
                   'PrecipGaugeLvl_Avg':missing_data_df[17].astype(float),
                   })
                
        # write new data to MySQL database
        new_row.to_sql(name='raw_mountmaya', con=engine, if_exists = 'append', index=False)
        
        # write current time for sanity check and exit loop
        current_dateTime = datetime.now()
        print("Done at:", current_dateTime, '- refreshing in 1 hour...')

# close mysql engine call
conn.close()
engine.dispose()