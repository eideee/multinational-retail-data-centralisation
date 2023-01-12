import yaml
import psycopg2
import pandas as pd
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine.url import URL
import sqlalchemy as sql

file_path = "C:/Users/Idi/Multinational_Retail_Data/db_creds.yaml"
local_engine = 'postgresql://postgres:Fiber101@localhost:5432/Sales_Data'
class DatabaseConnector:

    def read_db_creds():
        with open(file_path, 'r') as file_descriptor:
            data = yaml.safe_load(file_descriptor)
        return data

    def init_db_engine(data):
        url = URL.create(**data)
        engine = create_engine(url, echo=True)
        return engine

    def list_db_tables(engine):
        inspector = inspect(engine)
        schemas = inspector.get_schema_names()

        #for schema in schemas:
            #print("schema: %s" % schema)
        for table_name in inspector.get_table_names(schema='public'):
            print("Table: %s" % table_name)
            #type for table_name is <class 'str'>
            for column in inspector.get_columns(table_name, schema='public'):
                print("Column: %s" % column)
            # 
        # The code below would be another option to print all the table names     
        print(sql.inspect(engine).get_table_names())

    def upload_to_db(table_name :str, df):
        
        # estalish connection with local database 
        con_engine = create_engine(local_engine)
        con_engine.connect()
        
        df.to_sql(table_name, con=con_engine, if_exists='replace')
        con_engine.dispose