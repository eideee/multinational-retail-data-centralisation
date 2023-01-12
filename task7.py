# %%
import datetime
import pandas as pd
from datetime import datetime
from database_utils import DatabaseConnector 
from data_extraction import DatabaseExtractor
from data_cleaning import DataClean
import sqlalchemy as sql


if __name__ == '__main__':
     data = DatabaseConnector.read_db_creds()
     engine = DatabaseConnector.init_db_engine(data)
     pd.set_option('display.max_columns', None)

#%%
DatabaseConnector.list_db_tables(engine)

# %%
df7 = DatabaseExtractor.extract_rds_table('orders_table', engine)
engine.dispose()
     
# %%
df7.columns
#%%
#Remove column 'first_name', 'last_name' and '1'
DataClean.clean_orders_data(df7)

#%%
# Remove rows and columns that have null values on all of their columns
DataClean.clean_user_data(df7)

# %%
#Verify if there are rows or columns with Nan/NULL values
weird_value = df7[df7.eq('NULL').any(1)]
weird_value

#%%
df7.head(10)

# %%
# upload the table to Sales_Data postgres database
DatabaseConnector.upload_to_db('orders_table', df7)

# %%