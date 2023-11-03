#!/home/alex/.local/lib/python3.6/ # specify python installation on server
# -*- coding: utf-8 -*-
# version 1.0.0

# This code reads in the live SWARM satellite messages from Stephanie Wx station, 
# checks the latest CHRL SQL entry on the 'raw' database and if the latest 
# record is not on the SQL database, it pushes the new data to appropriate 'raw'
# SQL database for VIU-Hydromet. 
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
        msg.append(base64.b64decode(item['data']).decode('ascii') + ',' + item['hiveRxTime'])

# get message data into dataframe and clean to match standard output
df_sat = pd.DataFrame([sub.split(",") for sub in msg[::-1]])
    
# filter only lon/lat for specific wx station
# Steph 6
s6_lat = '50.319'
s6_lon = '126.35'
coords_s6 = pd.DataFrame(index=range(len(df_sat)),columns=[4, 5])
coords_s6[4] = coords_s6[4].fillna(s6_lat)
coords_s6[5] = coords_s6[5].fillna(s6_lon)
df_coords = df_sat[[4,5]]
df_logical = df_coords.eq(coords_s6)
df_s6 = df_coords[df_logical]
idx = df_s6[df_logical[5]].index.tolist()
df_s6 = df_sat.iloc[idx]
df_s6 = df_s6.reset_index(drop=True)

# Steph 9
s9_lat = '50.345'
s9_lon = '120.362'
coords_s9 = pd.DataFrame(index=range(len(df_sat)),columns=[4, 5])
coords_s9[4] = coords_s9[4].fillna(s9_lat)
coords_s9[5] = coords_s9[5].fillna(s9_lon)
df_coords = df_sat[[4,5]]
df_logical = df_coords.eq(coords_s9)
df_s9 = df_coords[df_logical]
idx = df_s9[df_logical[5]].index.tolist()
df_s9 = df_sat.iloc[idx]
df_s9 = df_s9.reset_index(drop=True)
df_s9 = df_s9[498:].reset_index(drop=True)

# calculate water year for Stephanies (new year starts on 10.01.YYYY). 
# If months are before October, do nothing. Else add +1
WatYrs_s6 = []
for i in range(len(df_s6)):
    if int(str(df_s6[18].iloc[i]).split('-')[1]) < 10:
        WatYr = int(str(df_s6[18].iloc[i]).split('-')[0])
    else:
        WatYr = int(str(df_s6[18].iloc[i]).split('-')[0])+1
    WatYrs_s6.append(WatYr)
    
WatYrs_s9 = []
for i in range(len(df_s9)):
    if int(str(df_s9[14].iloc[i]).split('-')[1]) < 10:
        WatYr = int(str(df_s9[14].iloc[i]).split('-')[0])
    else:
        WatYr = int(str(df_s9[14].iloc[i]).split('-')[0])+1
    WatYrs_s9.append(WatYr)

# make sure you sort messages from older to newer dates as satellite sometimes 
# sends multiple records at same time which are not sorted from older to newer
df_s6 = df_s6.sort_values(by=[0,1,2]) # sort by columns YYYY, MM, DD, HH
df_s9 = df_s9.sort_values(by=[0,1,2]) # sort by columns YYYY, MM, DD, HH

# put datetime column together based on individual columns
s6_dt = df_s6[[ 0, 1, 2]].astype(str).astype(np.int64)
s6_dt = pd.concat([pd.DataFrame(WatYrs_s6),s6_dt], axis=1)
s6_dt.columns = ["year","month","day","hours"]
s6_dt = pd.to_datetime(s6_dt)

s9_dt = df_s9[[ 0, 1, 2]].astype(str).astype(np.int64)
s9_dt = pd.concat([pd.DataFrame(WatYrs_s9),s9_dt], axis=1)
s9_dt.columns = ["year","month","day","hours"]
s9_dt = pd.to_datetime(s9_dt)

# combine dataframes into list
steph_master = [df_s6,df_s9]
steph_master_dt = [s6_dt,s9_dt]

# read existing SQL entry with data and check if new data needs writing
# reading the 'raw' SQL results in Memory Error messages due to 
# size. This process differs from 'clean' where we first need to
# read the SQL database using '_query' and setting a limit of 1000 rows
stephanies = [6,9] # the number associated with the stephanie stations connected to GOES
for i in range(len(stephanies)):  
    while True:
        print('Checking for new data from satellite for Steph %s' %(stephanies[i])) 
        
        # note Steph 9 is Upper Russell here
        if stephanies[i] == 6:
            sql_file = pd.read_sql_query(sql="SELECT * FROM raw_steph%s ORDER BY DateTime DESC LIMIT 1000" %(stephanies[i]), con = engine)
        else:
            sql_file = pd.read_sql_query(sql="SELECT * FROM raw_upperrussell ORDER BY DateTime DESC LIMIT 1000", con = engine)
        
        last_dt_sql = sql_file['DateTime'].iloc[0] # index [0] as newer data at top
        last_dt_system = steph_master_dt[i].iloc[-1]
        
        # if the last row in SWARM matches last row in SQL database (i.e. no new data to
        # write), then exit and don't write new data to database
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
            if last_dt_sql < steph_master_dt[i].iloc[0]:
                # safeguard in case the latest data on SQL is before the satelite 
                # record started (should only happen when satelite connection 
                # established for first time)
                last_idx = len(steph_master[i])
            else:
                # else calculate latest SQL entry and assess how many new 
                # satellite data to write to SQL database
                last_dt_sql_idx = int(np.flatnonzero(last_dt_sql == steph_master_dt[i])[0])
                last_idx = steph_master[i].index[-1] - last_dt_sql_idx
                
            # only keep new data that needs added to sql database
            missing_data_df = steph_master[i].iloc[-last_idx:]
            missing_data_dt = steph_master_dt[i].iloc[-last_idx:]
            
            # export new data to last row of SQL database  
            # No data values will automatically be added in SQL database as 
            # 'NULL'
            # for Steph 6
            if stephanies[i] == 6:
                new_row = pd.DataFrame({'DateTime':missing_data_dt,
                           'WatYr':WatYrs_s6[-last_idx:],
                           'Batt':missing_data_df[6].astype(float),
                           'Air_Temp':missing_data_df[7].astype(float),
                           'RH':missing_data_df[8].astype(float),
                           'Wind_speed':missing_data_df[9].astype(float),
                           'Pk_Wind_Speed':missing_data_df[10].astype(float),
                           'Wind_Dir':missing_data_df[11].astype(float),
                           'Wind_Dir_SD':missing_data_df[12].astype(float),
                           'Solar_Rad':missing_data_df[13].astype(float),
                           'Snow_Depth': missing_data_df[14].astype(float), 
                           'SDist_Q':missing_data_df[15].astype(float),
                           'PP_Tipper':missing_data_df[16].astype(float),
                           'PC_Raw_Pipe':missing_data_df[17].astype(float)
                           })
                # write new data to MySQL database
                new_row.to_sql(name='raw_steph%s' %stephanies[i], con=engine, if_exists = 'append', index=False)
            
            # for Steph 9
            else:
                new_row = pd.DataFrame({'DateTime':missing_data_dt,
                           'WatYr':WatYrs_s9[-last_idx:],
                           'Batt':missing_data_df[6].astype(float),
                           'Air_Temp':missing_data_df[7].astype(float),
                           'RH':missing_data_df[8].astype(float),
                           'PP_Tipper':missing_data_df[9].astype(float),
                           'PP_Tipper_cnt':missing_data_df[10].astype(float),
                           'PC_Raw_Pipe':missing_data_df[11].astype(float),
                           'River_Thick':missing_data_df[12].astype(float),
                           'River_Thick_SD':missing_data_df[13].astype(float),
                           })
                # write new data to MySQL database
                new_row.to_sql(name='raw_upperrussell', con=engine, if_exists = 'append', index=False)
            
            # write current time for sanity check and exit loop
            current_dateTime = datetime.now()
            print("Done at:", current_dateTime, '- refreshing in 1 hour...')