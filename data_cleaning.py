import pandas as pd
import re

class DataClean:

    # NULL values
    def clean_user_data(table_name):
        table_name.dropna(axis=0, how='all', inplace=True)
        table_name.dropna(axis=1, how='all', inplace=True)
    
    def clean_card_data(table_name):
        table_name.dropna(axis=0, how='all', inplace=True)
        table_name.dropna(axis=1, how='all', inplace=True)

    def clean_store_data(table_name):
        table_name.dropna(axis=0, how='all', inplace=True)
        table_name.dropna(axis=1, how='all', inplace=True)

    def clean_products_data(table_name):
        table_name.dropna(axis=0, how='all', inplace=True)
        table_name.dropna(axis=1, how='all', inplace=True)
        
    def convert_product_weights(weight):
        if re.search(r'kg\b', weight):
            x = re.sub("[\s,'kg']", "", weight)
            return float(x)
        elif re.search ('x', weight):
            weight = re.sub("x", "*", weight)
            y = re.sub("[\s,'g']", "", weight)
            z = eval(y)
            return float(z)/1000
        elif re.search (r'ml\b',weight):
            x = re.sub("[\s,'ml']", "", weight)
            return float(x)/1000
        elif re.search(r'g\b', weight):
            x = re.sub("[\s,'g']", "", weight)
            return float(x)/1000

    def clean_orders_data(dframe):
        dframe.drop(['first_name', 'last_name', '1'], axis=1, inplace=True)

    def clean_date_data(dframe):
        dframe.dropna(axis=0, how='all', inplace=True)
        dframe.dropna(axis=1, how='all', inplace=True)
        