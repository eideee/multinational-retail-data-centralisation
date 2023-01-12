#%%
import requests
import pandas as pd
import json
from data_extraction import DatabaseExtractor
from database_utils import DatabaseConnector
from data_cleaning import DataClean

if __name__ == '__main__':
    api_key = {'x-api-key' : 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
    no_of_stores = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
    get_stores = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/'

#%%
r1 = DatabaseExtractor.list_number_of_stores(no_of_stores, api_key)
#%%
#Verify the answer
print(type(r1))
r1
#%%
#Retrieve the total number of stores
print(r1['number_stores'])
#%%
#Retrieve details for each store and combine them into single dictionary
r2 = DatabaseExtractor.retrieve_stores_data(f"{get_stores}{(r1['number_stores'] - 1)}", api_key)
#%%
#Verify the contents
r2

#%%
#Create list to store parsed data
store_dta=[]

#%%
#DatabaseExtractor.retrieve_stores_data created a two layer dictionary
#the code below would parse each layer and update the values into the list
for i in r2.items():
    store_dta.append([i[0], i[1]])

#%%
#Pass the list to a dataframe and create two columns to represent the nested dictionary
df1 = pd.DataFrame(store_dta, columns=['No', 'Store'])

#%%
#Verify the content of the dataframe
df1

#%%
#Explore option to flatten the data
df1.explode('Store')

#%%
#Create another dataframe that has the content of normalized json data
df2 = pd.json_normalize(json.loads(df1.to_json(orient="records")))

#%%
#Verify the content of the newly created dataframe
df2

#%%
#remove redundant columns
df2.drop(['No','Store.lat'], axis=1, inplace=True)
#%%
#Clean the Nan/NULL values from the dataframe
DataClean.clean_store_data(df2)

#%%
#The above dropna method did not remove rows with all NULL value, possibly because there are certain rows that have mixed values
#The code below is an attempt to remove them by specifying main columns as subset
df2.dropna(subset=['Store.address', 'Store.opening_date'], inplace=True)

#%%
#Replace typo of eeEurope and eeAmerica
df2.replace("eeEurope", "Europe", inplace=True)
df2.replace("eeAmerica", "America", inplace=True)

#%%
#Verify if thera are rows which all of their values are NaN/NULL
null_value = df2[df2.eq('NULL').any(1)]
null_value

#%%
df2.drop([217, 405, 437], axis=0, inplace=True)

#%%
#Remove rows with meaningless values. UUID value should be 36 characters
long_char = []
for ind in df2.index:
    if (len(df2['Store.address'][ind]) == 10) & (len(df2['Store.store_code'][ind]) == 10) &(len(df2['Store.opening_date'][ind]) == 10):
        long_char.append(ind)

#%%
long_char
#%%
df2.drop(long_char , inplace=True)

#%%
#change the date to be datetime data type and unify the date format
df2['Store.opening_date'] = pd.to_datetime(df2['Store.opening_date'])

#%%
df2['Store.opening_date'] = df2['Store.opening_date'].dt.strftime('%Y-%m-%d')

#%%
df2.info()

#%%
df2.columns

#%%
#Remove characters that are not numbers all number of staff should only be numbers 
df2['Store.staff_numbers'] = df2['Store.staff_numbers'].str.extract('(\d+)', expand=False)

#%%
#Change the columns name
df3 = df2.rename(columns={'Store.index':'index', 'Store.address':'address', 'Store.longitude':'longitude', 'Store.locality':'locality',
       'Store.store_code':'store_code', 'Store.staff_numbers':'staff_numbers', 'Store.opening_date':'opening_date',
       'Store.store_type':'store_type', 'Store.latitude':'latitude', 'Store.country_code':'country_code',
       'Store.continent':'continent'})

#%%
df3.columns
#%%
#Upload the cleaned dataframe to postgres database
DatabaseConnector.upload_to_db('dim_store_details', df3)

#%%