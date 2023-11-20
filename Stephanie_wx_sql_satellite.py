#!/home/alex/.local/lib/python3.6/ # specify python installation on server
# -*- coding: utf-8 -*-
# version 1.0.0

# This code reads in the 'raw' SQL database for Stephanie stations connected to
# SWARM satellite system and if the latest record is not on the 'clean' SQL 
# database, it pushes the new data to appropriate 'clean' SQL database 
# for VIU-Hydromet. 
# Written by J. Bodart

import pandas as pd 
import numpy as np
from datetime import datetime

# Establish a connection with MySQL database 'viuhydro_wx_data_v2'
# Server log-in details stored in config file
import config
engine = config.main_sql()

# check both 'raw' and 'clean' for each wx station and push if necessary
stephanies = [6,9] # the number associated with the stephanie stations connected to GOES
for i in range(len(stephanies)):  
    while True:
        print('Checking for new data from satellite for Steph %s' %(stephanies[i])) 
        
        # note Steph 9 is Upper Russell here
        if stephanies[i] == 6:
            sql_file_raw = pd.read_sql_query(sql="SELECT * FROM raw_steph%s ORDER BY DateTime DESC LIMIT 1000" %(stephanies[i]), con = engine)
            sql_file_clean = pd.read_sql_query(sql="SELECT * FROM clean_steph%s ORDER BY DateTime DESC LIMIT 1000" %(stephanies[i]), con = engine)
        else:
            sql_file_raw = pd.read_sql_query(sql="SELECT * FROM raw_upperrussell ORDER BY DateTime DESC LIMIT 1000", con = engine)
            sql_file_clean = pd.read_sql_query(sql="SELECT * FROM clean_upperrussell ORDER BY DateTime DESC LIMIT 1000", con = engine)
        
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
            
            # calculate water year for Stephanies (new year starts on 10.01.YYYY). 
            # If months are before October, do nothing. Else add +1
            WatYrs = []
            for j in range(len(missing_data_df)):
                if int(str(missing_data_dt.iloc[j]).split('-')[1]) < 10:
                    WatYr = int(str(missing_data_dt.iloc[j]).split('-')[0])
                else:
                    WatYr = int(str(missing_data_dt.iloc[j]).split('-')[0])+1
                WatYrs.append(WatYr)
            
            # export new data to last row of SQL database  
            # No data values will automatically be added in SQL database as 
            # 'NULL'
            # for Steph 6
            if stephanies[i] == 6:
                
                # calculate snow depth and correct when necessary
                snow_depth = missing_data_df['Snow_Depth'].astype(float)
                snow_depth = np.round(3.79 - snow_depth,2)*100 # m to cm
                    
                # write to SQL
                new_row = pd.DataFrame({'DateTime':missing_data_dt,
                           'WatYr':WatYrs,
                           'Batt':missing_data_df['Batt'].astype(float),
                           'Air_Temp':missing_data_df['Air_Temp'].astype(float),
                           'RH':missing_data_df['RH'].astype(float),
                           'Wind_Speed':missing_data_df['Wind_speed'].astype(float),
                           'Pk_Wind_Speed':missing_data_df['Pk_Wind_Speed'].astype(float),
                           'Wind_Dir':missing_data_df['Wind_Dir'].astype(float),
                           'Solar_Rad':missing_data_df['Solar_Rad'].astype(float),
                           'Snow_Depth': snow_depth, #distance to ground conversion
                           'PP_Tipper':missing_data_df['PP_Tipper'].astype(float),
                           'PC_Raw_Pipe':missing_data_df['PC_Raw_Pipe'].astype(float)*1000, # convert to mm
                           #'BP':missing_data_df['BP'].astype(float) # in kpa but needs fixing first - Sergey is on it
                           'BP': np.nan # in kpa but needs fixing first - Sergey is on it
                           })
                # write new data to MySQL database
                new_row.to_sql(name='clean_steph%s' %stephanies[i], con=engine, if_exists = 'append', index=False)
            
            # for Steph 9
            else:
                new_row = pd.DataFrame({'DateTime':missing_data_dt,
                           'WatYr':WatYrs,
                           'Batt':missing_data_df['Batt'].astype(float),
                           'Air_Temp':missing_data_df['Air_Temp'].astype(float),
                           'RH':missing_data_df['RH'].astype(float),
                           'PP_Tipper':missing_data_df['PP_Tipper'].astype(float),
                           'PC_Raw_Pipe':missing_data_df['PC_Raw_Pipe'].astype(float)*1000, # convert to mm
                           })
                # write new data to MySQL database
                new_row.to_sql(name='clean_upperrussell', con=engine, if_exists = 'append', index=False)
            
            # write current time for sanity check and exit loop
            current_dateTime = datetime.now()
            print("Done at:", current_dateTime, '- refreshing in 1 hour...')