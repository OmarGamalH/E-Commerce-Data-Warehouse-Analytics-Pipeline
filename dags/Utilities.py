import snowflake.connector as connector
import snowflake.connector.pandas_tools as sp
import pandas as pd
import os 
import logging as l

import datetime 


current_directory = os.getcwd()

data_directory = os.path.join(current_directory , 'dags' , 'Data')

print(data_directory)

conn = connector.connect(
    user = '<user>',
    password = '<password>',
    account = '<account>',
    warehouse = 'compute_wh',
    database = 'E_COMMERCE_DATABASE',
    schema = 'RAW'
)

cursor = conn.cursor()

logger = l.getLogger(__name__)

l.basicConfig(format = "%(asctime)s - %(levelname)s - %(name)s - %(message)s" , level = l.INFO)

def read_data(csv_name):
    try:
        logger.info(f"start extracting data from {csv_name}...")
        full_csv_file = 'olist_' + csv_name + '_dataset.csv'

        csv_directory = os.path.join(data_directory , full_csv_file)

        df = pd.read_csv(csv_directory)
        logger.info(f"Extraction of {csv_name} has ended successfully")
        return df
    except Exception as e:
        logger.error(f'Error in extraction has occured in file {csv_name}: {e}')
        raise e



def load_data( conn , df : pd.DataFrame , table_name : str):
    try:
        l.info(f"Start loading of data to {table_name}")
        # df = df.assign(updated_at = datetime.datetime.now())
        
        columns = [*map(str.upper , list(df.columns))]
        df.columns = columns

        sp.write_pandas(conn , df , table_name.upper())
        l.info(f'Loading of data to {table_name} has ended successfully')
    except Exception as e:
        logger.error(f"Error in loading has occured of table {table_name}: {e}")
        raise e


def delete_data(cursor , table_name : str):
        try:
            l.info(f"Start deleting of data from {table_name}")
            

            cursor.execute(f"DELETE FROM {table_name.upper()}")

            l.info(f'deleting of data from {table_name} has ended successfully')
        except Exception as e:
            logger.error(f"Error in deletion has occured in table {table_name}: {e}")
            raise e


def from_csv_to_snowflake(conn, name):
    data_df = read_data(name)
    load_data(conn , data_df , name )


# tables = ['category_name' , 'customers' , 'geolocation' , 'order_items' , 'order_payments' , 'order_reviews' , 'orders' , 'products' , 'sellers']
# for table_name in tables:

#     delete_data(cursor , table_name)
#     df = read_data(table_name)

#     load_data(conn , df , table_name)

# df = read_data('products')
# df = df.assign(updated_at = datetime.datetime.now())

# print(df.info())

# print(df)
