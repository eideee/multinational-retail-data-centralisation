#%%
import pandas as pd
import numpy as np
from data_extraction import DatabaseExtractor
from database_utils import DatabaseConnector
from data_cleaning import DataClean

#%%
#parse the pdf document by sending the http link as the parameter

if __name__ == '__main__':
    pdf_tab = DatabaseExtractor.retrieve_pdf_data("https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf")

#%%
#Verify the content of the parsed pdf document
pdf_tab
#%%
#assign the list to the dataframe
list1 = []
for item in pdf_tab:
    for info in item.values:
        list1.append(info)
#%%
list1
#%%
df = pd.DataFrame(list1)
#%%
#Verify the newly created dataframe
df.head()

#%%
#Rename the columns
df1 = df.rename(columns = {0:'card_number', 1:'expiry_date', 2:'card_provider',
       3:'date_payment_confirmed'})

#%%
df1.head()
#%%
#Remove columns and rows that have all of their values as NaN/NaT/NULL
DataClean.clean_card_data(df1)

# %%
#The above dropna method did not remove rows with all NULL value
#The code below is an attempt to remove them by specifying all the columns as subset
df1.dropna(subset=['card_number', 'expiry_date', 'card_provider'], inplace=True)

#%%
df1.head()
#%%
weird_char = []
for ind in df1.index:
    if len(df1['card_provider'][ind]) == len(df1['expiry_date'][ind]):
        weird_char.append(ind)

#%%
weird_char

#%%
#Verify that the entire row has dirty values
df1.loc[weird_char]
#%%
df1.drop(weird_char , inplace=True)

#%%
#Upload the cleaned dataframe to postgres database
DatabaseConnector.upload_to_db('dim_card_details', df1)

#%%