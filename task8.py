#%%
import pandas as pd
import json
import requests
from database_utils import DatabaseConnector 
from data_extraction import DatabaseExtractor
from data_cleaning import DataClean
from datetime import datetime

if __name__ == '__main__':
    dt_link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'
    df = DatabaseExtractor.extract_json_data(dt_link)

#%%
df.info()

# %%
df.head(5)
#%%
DataClean.clean_date_data(df)

#%%
weird_char = []
for ind in df.index:
    if len(df['timestamp'][ind]) == len(df['month'][ind]) & len(df['time_period'][ind]) == len(df['date_uuid'][ind]):
        weird_char.append(ind)

#%%
weird_char

#%%
#Verify that the entire row has dirty values
df.loc[weird_char]
#%%
df.drop(weird_char, inplace=True)

#%%
#Create a new 'date' column that unify the data from year, month and day columns
cols=["year", "month", "day", "timestamp"]
df['complete_time'] = df[cols].apply(lambda x: '-'.join(x.values.astype(str)), axis=1)

#%%
#cast date column to be datetime data type
df['complete_time']=pd.to_datetime(df['complete_time'])
df.info()

#%%
df.head()
#%%
'''
# The following codes are optional. It was used to change
# the time format from 24Hrs to 12Hrs
d = df['timestamp'][0]
print(d)
#%%
# %%
# convert the 24 hour time to 12 hour time
new_time = []
for ind in df.index:
    t = df['timestamp'][ind]
    d = datetime.strptime(t, "%H:%M:%S")
    new_time.append(d.strftime("%I:%M:%S %p"))

#%%
df['time'] = new_time

'''
#%%
#Upload the cleaned dataframe to postgres database
DatabaseConnector.upload_to_db('dim_date_times', df)

#%%
