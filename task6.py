# %%
import pandas as pd
import numpy as np
import re
from database_utils import DatabaseConnector 
from data_extraction import DatabaseExtractor
from data_cleaning import DataClean


if __name__ == '__main__':
    path = 's3://data-handling-public/products.csv'
    df = DatabaseExtractor.extract_from_s3(path)
#%%
#Verify the newly created dataframe
df.head(10)

#%%
#Remove all rows and columns that have all of their contents' as NaN/NULL
DataClean.clean_products_data(df)

#%%
#Verify if Nan/NULL values present on the entire row
null_value = df[df.isna().any(1)]
null_value

#%%
#The above dropna method did not remove rows with all NULL value, possibly because there are certain rows that have mixed values
#The code below is an attempt to remove them by specifying main columns as subset
df.dropna(subset=['product_name', 'product_code'], inplace=True)

#%%
#Remove rows with meaningless values
long_char = []
for ind in df.index:
    if len(df['uuid'][ind]) <= 11:
        long_char.append(ind)

#%%
long_char

#%%
#Verify that the entire row has dirty values
df.loc[1400]
#%%
df.drop(long_char , inplace=True)

#%%
#change the date to be datetime data type and unify the date format
df['date_added'] = pd.to_datetime(df['date_added'])

#%%
df.drop(1400, axis=0, inplace=True)

#%%
#Check every item on weight's column and convert it to be kilogram
df['weight'] = df['weight'].apply(DataClean.convert_product_weights)

#%%
#Upload the cleaned dataframe to postgres database
DatabaseConnector.upload_to_db('dim_products', df)

#%%
