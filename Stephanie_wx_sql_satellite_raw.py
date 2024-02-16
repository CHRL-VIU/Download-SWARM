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
        #msg.append(base64.b64decode(item['data']).decode('ascii') + ',' + item['hiveRxTime'])
        msg.append(base64.b64decode(item['data']).decode('ascii'))

# get message data into dataframe and clean to match standard output
df_sat = pd.DataFrame([sub.split(",") for sub in msg[::-1]])
    
# filter only lon/lat for specific wx station
# Steph 6
s6 = 'S6'
label_s6 = pd.DataFrame(index=range(len(df_sat)),columns=[1])
label_s6 = label_s6.fillna(s6)
df_label = df_sat[[0]]
df_logical = df_label.eq(label_s6.values)
df_s6 = df_label[df_logical]
idx = df_s6[df_logical[0]].index.tolist()
df_s6 = df_sat.iloc[idx]
df_s6 = df_s6.reset_index(drop=True)

# Steph 9
s9 = 'S9'
label_s9 = pd.DataFrame(index=range(len(df_sat)),columns=[1])
label_s9 = label_s9.fillna(s9)
df_label = df_sat[[0]]
df_logical = df_label.eq(label_s9.values)
df_s9 = df_label[df_logical]
idx = df_s9[df_logical[0]].index.tolist()
df_s9 = df_sat.iloc[idx]
df_s9 = df_s9.reset_index(drop=True)

#%% Steph 6
# split data by dates and hours
dates_s6 = df_s6.iloc[:,1:4]
hours_s6 = df_s6.iloc[:,[4,17]]

# merge dates and years with data for each two hour slots
df_s6_1 = pd.concat([dates_s6, hours_s6.iloc[:,0].str.replace('h',''),df_s6.iloc[:,5:17]], axis=1)
df_s6_2 = pd.concat([dates_s6, hours_s6.iloc[:,1].str.replace('h',''),df_s6.iloc[:,18:30]], axis=1)

# convert to datetime
s6_1_dt = df_s6_1[[1,2,3,4]].astype(str).astype(np.int64)
s6_1_dt.columns = ["year","month","day","hours"]
s6_1_dt = pd.to_datetime(s6_1_dt)

s6_2_dt = df_s6_2[[1,2,3,17]].astype(str).astype(np.int64)
s6_2_dt.columns = ["year","month","day","hours"]
s6_2_dt = pd.to_datetime(s6_2_dt)

# fix issues at midnight for second hourly message
idx_midnight = np.flatnonzero(df_s6_2.iloc[:,3] == '00')
s6_2_dt[idx_midnight] = pd.DatetimeIndex(s6_2_dt[idx_midnight]) + pd.DateOffset(1)

# merge together datetimes and df_s6_1 and df_s6_2
df_s6_1 = pd.concat((s6_1_dt, df_s6_1.iloc[:,4:]), axis=1).reset_index(drop=True).T.reset_index(drop=True).T
df_s6_2 = pd.concat((s6_2_dt, df_s6_2.iloc[:,4:]), axis=1).reset_index(drop=True).T.reset_index(drop=True).T

df_s6 = pd.concat([df_s6_1, df_s6_2])
df_s6 = df_s6.sort_values(by=[0]).reset_index(drop=True)

#%% Steph 9
# split data by dates and hours
dates_s9 = df_s9.iloc[:,1:4]
hours_s9 = df_s9.iloc[:,[4,13]]

# merge dates and years with data for each two hour slots
df_s9_1 = pd.concat([dates_s9, hours_s9.iloc[:,0].str.replace('h',''),df_s9.iloc[:,5:13]], axis=1)
df_s9_2 = pd.concat([dates_s9, hours_s9.iloc[:,1].str.replace('h',''),df_s9.iloc[:,14:22]], axis=1)

# convert to datetime
s9_1_dt = df_s9_1[[1,2,3,4]].astype(str).astype(np.int64)
s9_1_dt.columns = ["year","month","day","hours"]
s9_1_dt = pd.to_datetime(s9_1_dt)

s9_2_dt = df_s9_2[[1,2,3,13]].astype(str).astype(np.int64)
s9_2_dt.columns = ["year","month","day","hours"]
s9_2_dt = pd.to_datetime(s9_2_dt)

# fix issues at midnight for second hourly message
idx_midnight = np.flatnonzero(df_s9_2.iloc[:,3] == '00')
s9_2_dt[idx_midnight] = pd.DatetimeIndex(s9_2_dt[idx_midnight]) + pd.DateOffset(1)

# merge together datetimes and df_s9_1 and df_s9_2
df_s9_1 = pd.concat((s9_1_dt, df_s9_1.iloc[:,4:]), axis=1).reset_index(drop=True).T.reset_index(drop=True).T
df_s9_2 = pd.concat((s9_2_dt, df_s9_2.iloc[:,4:]), axis=1).reset_index(drop=True).T.reset_index(drop=True).T

df_s9 = pd.concat([df_s9_1, df_s9_2])
df_s9 = df_s9.sort_values(by=[0]).reset_index(drop=True)

#%% combine dataframes into list
steph_master = [df_s6,df_s9.reset_index(drop=True)]

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
        last_dt_system = steph_master[i].iloc[-1,0]
        
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
            if last_dt_sql < steph_master[i].iloc[0,0]:
                # safeguard in case the latest data on SQL is before the satelite 
                # record started (should only happen when satelite connection 
                # established for first time)
                last_idx = len(steph_master[i])
            else:
                # else calculate latest SQL entry and assess how many new 
                # satellite data to write to SQL database
                last_dt_sql_idx = int(np.flatnonzero(last_dt_sql == steph_master[i].iloc[:,0])[0])
                last_idx = steph_master[i].index[-1] - last_dt_sql_idx
                
            # only keep new data that needs added to sql database
            missing_data_df = steph_master[i].iloc[-last_idx:]
            missing_data_dt = steph_master[i].iloc[-last_idx:,0]
            
            # export new data to last row of SQL database  
            # No data values will automatically be added in SQL database as 
            # 'NULL'
            # for Steph 6
            if stephanies[i] == 6:
                new_row = pd.DataFrame({'DateTime':missing_data_dt,
                           'Batt':missing_data_df[1].astype(float),
                           'Air_Temp':missing_data_df[2].astype(float),
                           'RH':missing_data_df[3].astype(float),
                           'Snow_Depth': missing_data_df[4].astype(float), 
                           'Wind_speed':missing_data_df[5].astype(float),
                           'Pk_Wind_Speed':missing_data_df[6].astype(float),
                           'Wind_Dir':missing_data_df[7].astype(float),
                           'Wind_Dir_SD':missing_data_df[8].astype(float),
                           'PP_Tipper':missing_data_df[9].astype(float),
                           #'BP':missing_data_df[10].astype(float), # in kpa
                           'BP':np.nan, # in kpa but needs fixing first - Sergey is on it
                           'Solar_Rad':missing_data_df[11].astype(float),
                           'PC_Raw_Pipe':missing_data_df[12].astype(float)                         
                           })
                # write new data to MySQL database
                new_row.to_sql(name='raw_steph%s' %stephanies[i], con=engine, if_exists = 'append', index=False)
            
            # for Steph 9
            else:
                new_row = pd.DataFrame({'DateTime':missing_data_dt,
                           'Batt':missing_data_df[1].astype(float),
                           'Air_Temp':missing_data_df[2].astype(float),
                           'RH':missing_data_df[3].astype(float),
                           'PP_Tipper':missing_data_df[4].astype(float),
                           'PP_Tipper_cnt':missing_data_df[5].astype(float),
                           'PC_Raw_Pipe':missing_data_df[6].astype(float),
                           'River_Thick':missing_data_df[7].astype(float),
                           'River_Thick_SD':missing_data_df[8].astype(float),
                           })
                # write new data to MySQL database
                new_row.to_sql(name='raw_upperrussell', con=engine, if_exists = 'append', index=False)
            
            # write current time for sanity check and exit loop
            current_dateTime = datetime.now()
            print("Done at:", current_dateTime, '- refreshing in 1 hour...')