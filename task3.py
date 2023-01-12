# %%
import datetime
import pandas as pd
from database_utils import DatabaseConnector 
from data_extraction import DatabaseExtractor
from data_cleaning import DataClean


if __name__ == '__main__':
     data = DatabaseConnector.read_db_creds()
     engine = DatabaseConnector.init_db_engine(data)
     pd.set_option('display.max_columns', None)
     DatabaseConnector.list_db_tables(engine)

# %%
Tab_user = DatabaseExtractor.extract_rds_table('legacy_users', engine)
engine.dispose()
     
# %%
#Verify the newly created dataframe
Tab_user
#%%
# Remove rows that have null values on all of their columns
DataClean.clean_user_data(Tab_user)

# %%
null_value = Tab_user[Tab_user.eq('NULL').any(1)]
null_value
#%%
#The above dropna method did not remove rows with all NULL value, possibly because there are certain rows that have mixed values
#The code below is an attempt to remove them by specifying main columns as subset
Tab_user.dropna(subset=['first_name', 'last_name', 'user_uuid'], inplace=True)

#%%
#The code above did not remove the rows that have all of their values as NULL
#Another attempt to find and delete NULL rows
null_char = []
for ind in Tab_user.index:
    if ((Tab_user['first_name'][ind]) == 'NULL') & ((Tab_user['last_name'][ind]) == 'NULL') & ((Tab_user['user_uuid'][ind]) == 'NULL'):
        null_char.append(ind)

#%%
null_char
#%%
#remove rows that have all of their values as NULL
Tab_user.drop(null_char , inplace=True)

#%%
#Remove rows with meaningless values. UUID value should be 36 characters
long_char = []
for ind in Tab_user.index:
    if (len(Tab_user['user_uuid'][ind]) == 10) & (len(Tab_user['first_name'][ind]) == 10) &(len(Tab_user['last_name'][ind]) == 10):
        long_char.append(ind)

#%%
Tab_user.loc[long_char]

#%%
Tab_user[Tab_user.eq('569b002e-6886-4852-96e3-50bf000243bd'). any(1)]
#%%
Tab_user.drop(long_char , inplace=True)

#%%
Tab_user.dtypes

#%%
#change the date to be datetime data type and unify the date format
Tab_user['date_of_birth'] = pd.to_datetime(Tab_user['date_of_birth'])
Tab_user['date_of_birth'] = Tab_user['date_of_birth'].dt.strftime('%Y-%m-%d')

# %%
# upload the table to Sales_Data postgres database
DatabaseConnector.upload_to_db('dim_users', Tab_user)

# %%
