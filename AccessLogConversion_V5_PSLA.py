from asyncio.windows_events import NULL
from pickle import TRUE
from sqlite3 import Timestamp
from numpy import true_divide
import pandas as pd
import datetime
from datetime import date, time, timedelta
import sys
import json
import glob

def read_configuration(filename):
    # Opening JSON file
    f = open(filename)
  
    # returns JSON object as a dictionary
    configdata = json.load(f)
    print(configdata)
  
    # Closing file
    f.close()

    return configdata


# define function that will sort the rows of a given table in ascending order by given column name
def order_by_time(df_ToOrder, field_name, time_format):
    df_ToOrder['Timestamp'] = pd.to_datetime(df_ToOrder['Timestamp'], format=time_format)
    df_ToOrder.sort_values(field_name, axis = 0, ascending = True, inplace = True, na_position = 'last')
    return df_ToOrder

def assign_shift(df_ToAssignShift, dsStart, dsEnd, nsStart, nsEnd):
    for j in range(1, len(df_ToAssignShift)):
        
        ts_str = df_ToAssignShift.at[j, 'Timestamp']
        ts_str1 = ts_str.strftime("%H:%M")
        ts_str2 = ts_str.strftime("%m-%d")
       
        if (ts_str1 >= dsStart) and ( ts_str1 < dsEnd):
            df_ToAssignShift.at[j, 'Shift'] = ts_str2 + "-Day"
        elif (ts_str1 < dsStart) :
            day_delta = pd.to_datetime(ts_str) - timedelta(days=1)
            ts_str2 = day_delta.strftime("%m-%d")
            df_ToAssignShift.at[j, 'Shift'] = ts_str2 + "-Night"
        else:
            df_ToAssignShift.at[j, 'Shift'] = ts_str2 + "-Night"
    return df_ToAssignShift         

################################################################################
############### Setup and Initialize the main dataframe table ##################
################################################################################
# Read and assign configuration from json file
#confFilename = sys.argv[1]
# print(confFilename)
confFilename = "AccessLogConversionConf_XOMBR-PSLA7_V0809.json"
#confFilename = "AccessLogConversionConf_XOMBR-EC_V0809.json"

conf_data = read_configuration(confFilename)

# Grab the input file name from the config data
##logfilename = conf_data["InputLogFile"] + ".csv"
logfilename = conf_data["InputLogFile"]

##url = 'https://raw.githubusercontent.com/kbryantUSL/AccessLogConversion/main/Dooractivities/DoorActivitiesPSLA100126night.csv'
# Read the file as a csv and insert the data in a data frame
df_original = pd.read_csv(logfilename)

#Create a header for the table
df_original.columns = ['Manway', 'Access Point', 'First Name', 'Last Name', 'Timestamp', 'Company', 'Structure', 'Card ID', 'Craft']

archive_now = datetime.datetime.now().strftime('%Y%m%d-%H%M%S')
new_file_name = conf_data["OutputArchiveFile"] + archive_now + ".csv"
print("Archive File Saved")
## Export the dataframe to the file
df_original.to_csv(new_file_name)


# Delete the first row of the table then copy the table into a new dataframe container
df_formatted = df_original.drop(labels=range(0, 0),axis=0)

# Add new columns to the table
df_formatted['Exit Timestamp'] = None
df_formatted['HCValue'] = None
df_formatted['TripID'] = None
df_formatted['Shift'] = None

# Initialize the HCValue and TripID columns
df_formatted.loc[:,'HCValue'] = 0
df_formatted.loc[:,'TripID'] = 0

# Order the rows chronologically
df_formatted = order_by_time(df_formatted,"Timestamp", conf_data["DateFormat"])

# Establish key shift time values; this should be grabbed from JSON file
dayShiftStartTime = conf_data["ShiftTimes"]["DayShiftStart"]
dayShiftEndTime = conf_data["ShiftTimes"]["DayShiftEnd"]
nightShiftStartTime = conf_data["ShiftTimes"]["NightShiftStart"]
nightShiftEndTime = conf_data["ShiftTimes"]["NightShiftEnd"]

# Populate the Shift column in the table
df_formatted = assign_shift(df_formatted, dayShiftStartTime, dayShiftEndTime, nightShiftStartTime, nightShiftEndTime)

################################################################################
#################### Main program variables initialization #####################
################################################################################

# tripCounter will keep track of paired INs and OUTs in the  Access Point Column
tripCounter = 0                 # This keeps the track of the total number of trips
df_length = len(df_formatted)   # assign length of main table to a variable

#print("Before loop")
#print("The DF length is ", df_length)
# This is the main loop. Find each Reader - In row in this chronologically sorted table, and then find it's associated Reader - Out
for i in range(0,df_length):
    if (df_formatted.at[df_formatted.index[i], 'Access Point'] == "Reader - In"):
        rowIndexToCheck = i+1
        exitEventFound = False

        #print("Row Index ", rowIndexToCheck)

        while ((not (exitEventFound)) and (df_length > rowIndexToCheck)):
            if (df_formatted.at[df_formatted.index[rowIndexToCheck], 'Access Point'] == "Reader - Out"):
                #print("In Reader - Out check ")
                if (df_formatted.at[df_formatted.index[rowIndexToCheck], 'TripID'] == 0):
                    #print("In TripID check ")
                    if (df_formatted.at[df_formatted.index[rowIndexToCheck], 'Card ID'] == df_formatted.at[df_formatted.index[i], 'Card ID']):
                        if (df_formatted.at[df_formatted.index[rowIndexToCheck], 'Structure'] == df_formatted.at[df_formatted.index[i], 'Structure']): 
                            if (df_formatted.at[df_formatted.index[rowIndexToCheck], 'Shift'] == df_formatted.at[df_formatted.index[i], 'Shift']):
                                
                                time_difference = df_formatted.at[df_formatted.index[rowIndexToCheck], 'Timestamp'] - df_formatted.at[df_formatted.index[i], 'Timestamp']
                                if (time_difference) < timedelta(hours=12):
                               
                                    exitEventFound = True
                                    tripCounter += 1
                                    df_formatted.at[df_formatted.index[i], 'TripID'] = tripCounter
                                    df_formatted.at[df_formatted.index[rowIndexToCheck], 'TripID'] = tripCounter
                                    df_formatted.at[df_formatted.index[i], 'HCValue'] = 1
                                    df_formatted.at[df_formatted.index[rowIndexToCheck], 'HCValue'] = -1
                                    df_formatted.at[df_formatted.index[i], 'Exit Timestamp'] = df_formatted.at[df_formatted.index[rowIndexToCheck], 'Timestamp']
                                    
                    
            rowIndexToCheck +=1



## Capture the current time in the format shown
now = datetime.datetime.now().strftime('%Y%m%d-%H%M%S')

## Name the file that we will save the dataframe to
file_name = conf_data["OutputLogFile"] + now + ".csv"
print("Daily Log File Saved")
## Export the dataframe to the file
df_formatted.to_csv(file_name)
#print(df_formatted)
print("End of execution")
#save_access_log_table(df_formatted)

path = conf_data["Path"]
csv_files = glob.glob(path)
print(csv_files)

df = pd.DataFrame()

for file in csv_files:
    data = pd.read_csv(file)
    df = pd.concat([df, data], axis = 0)

df.to_csv(conf_data["MasterFile"], index = False)