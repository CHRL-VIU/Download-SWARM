#!/home/alex/.local/lib/python3.6/ # specify python installation on server
# -*- coding: utf-8 -*-
# version 1.0.0

# This code reads in the live SWARM satellite messages from Mt Maya Wx station, 
# checks the latest CHRL SQL entry on the database and if the latest record is
# not on the SQL database, it pushes the new data to 'clean_maya' SQL database 
# for VIU-Hydromet. 
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

while True:
    # get message data into dataframe and clean to match standard output
    sql_file_raw = pd.read_sql_query(sql="SELECT * FROM raw_mountmaya ORDER BY DateTime DESC LIMIT 1000", con = engine)
    sql_file_clean = pd.read_sql_query(sql="SELECT * FROM clean_mountmaya ORDER BY DateTime DESC LIMIT 1000", con = engine)
    
    # get datetime for both clean and raw sql databases
    last_dt_sql_raw = sql_file_raw['DateTime'].iloc[0]
    last_dt_sql_clean = sql_file_clean['DateTime'].iloc[0]
    
    # if the last row in raw matches last row in clean SQL database 
    # (i.e. no new data to write), then exit and don't write new data
    check = last_dt_sql_raw == last_dt_sql_clean    
    if check:
        print('No new data detected - check satellite transmission?')  
        
        # write current time for sanity check and exit loop
        current_dateTime = datetime.now()
        print("Done at:", current_dateTime, '- refreshing in 1 hour...')
        break

    # else if new data on raw which is not yet written to clean, write it
    else:
        print('New satellite data detected - writing to clean database') 
        
        # calculate latest SQL entry and assess how many new 
        # satellite data to write to SQL database
        last_dt_sql_idx = int(np.flatnonzero(last_dt_sql_clean == sql_file_raw['DateTime']))
        last_idx = (sql_file_raw.index[0] - last_dt_sql_idx)
            
        # only keep new data that needs added to sql database
        missing_data_df = sql_file_raw.iloc[:-last_idx]
        missing_data_dt = sql_file_raw['DateTime'].iloc[:-last_idx]   
    
        # calculate PP_pipe
        pp_pipes = []
        for i in range(len(missing_data_df)):
            if i == 0:
                pp_pipe = 0
            else:
                pp_pipe = (float(missing_data_df['PrecipGaugeLvl_Avg'].iloc[i]) - float(missing_data_df['PrecipGaugeLvl_Avg'].iloc[i-1]))*1000
            pp_pipes.append(pp_pipe)
        
        # calculate water year (new year starts on 10.01.YYYY). If months are 
        # before October, do nothing. Else add +1
        WatYrs = []
        for i in range(len(missing_data_df)):
            if int(str(missing_data_df['DateTime'].iloc[i]).split('-')[1]) < 10:
                WatYr = int(str(missing_data_df['DateTime'].iloc[i]).split('-')[0])
            else:
                WatYr = int(str(missing_data_df['DateTime'].iloc[i]).split('-')[0])+1
            WatYrs.append(WatYr)  
                
        # convert "distance to snow" to snow depth by substracting height of
        # instrument above summer ground (3.8 m) and convert to cm
        snow_depth = missing_data_df['TCDT_Avg'].astype(float)
        snow_depth = (3.8-snow_depth)*100 # approx. height of tower instrument in summer
        snow_depth = np.round(snow_depth,2) # round to nearest 2 decimals
        
        # export new data to last row of SQL database  
        # No data values will automatically be added in SQL database as 
        # 'NULL'
        new_row = pd.DataFrame({'DateTime':missing_data_df['DateTime'],
            'WatYr':WatYrs,
            'Air_Temp':missing_data_df['AirTC_Avg'].astype(float),
            'RH':missing_data_df['RH_Avg'].astype(float),
            'BP':missing_data_df['BaroP_Avg'].astype(float),
            'Wind_speed':missing_data_df['WS_ms_Avg'].astype(float)*3.6, # convert m/s to km/h
            'Wind_Dir':missing_data_df['WindDir_D1_WVT'].astype(float),
            'Pk_Wind_Speed':missing_data_df['WS_ms_Max'].astype(float)*3.6, # convert m/s to km/h
            #'PC_Tipper':0, # no PC_Tipper but throwing error msg
            'PP_Tipper':missing_data_df['Rain_mm_Tot'].astype(float),
            'PC_Raw_Pipe':missing_data_df['PrecipGaugeLvl_Avg'].astype(float)*1000,
            'PP_Pipe':pp_pipes,
            'Snow_Depth':snow_depth,
            'Solar_Rad':missing_data_df['SolarRad_Avg'].astype(float),
            'Batt':missing_data_df['BattV_Avg'].astype(float),
            })
                
        # write new data to MySQL database
        new_row.to_sql(name='clean_mountmaya', con=engine, if_exists = 'append', index=False)
        
        # write current time for sanity check and exit loop
        current_dateTime = datetime.now()
        print("Done at:", current_dateTime, '- refreshing in 1 hour...')