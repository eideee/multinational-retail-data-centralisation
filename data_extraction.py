import pandas as pd
import tabula
import requests
import json
import boto3


class DatabaseExtractor:

    def extract_rds_table(table_name, engine):
        # Read data from SQL table
        sql_data = pd.read_sql_table(table_name,engine)
        return sql_data
        
    def retrieve_pdf_data(link: str):
        dfs = tabula.read_pdf(link, pages='all')
        return dfs

    def list_number_of_stores(no_of_stores: str, api_key: dict):
        response = requests.get(no_of_stores, headers = api_key)
        return response.json()

    def retrieve_stores_data(retrive_store_URL: str, api_key: dict):
        #data = requests.get(retrive_store_URL, headers = api_key)
        #print(retrive_store_URL[-3:])
        x = 0
        Dict = {}
        while x <= int(retrive_store_URL[-3:]):
            data = requests.get(retrive_store_URL[:-3] + str(x), headers = api_key)
            data_dict = data.json()
            Dict[x+1] = data_dict
            x = x + 1

        return Dict

    def extract_from_s3(path):
        client = boto3.client('s3')
        df = pd.read_csv(path)
        return df

    def extract_json_data(dt_link):
        response = requests.get(dt_link)
        data_to_explore = response.json()
        df = pd.DataFrame.from_dict(data_to_explore)
        return df

    