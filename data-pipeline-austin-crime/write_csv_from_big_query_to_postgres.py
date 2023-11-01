"""
Downloads the csv file from Big Query. Creates a new table in the Postgres server.
Reads the file as a dataframe and inserts each record to the Postgres table. 
"""
import psycopg2
import os
import traceback
import logging
import pandas as pd
import numpy as np
from datetime import datetime
from google.cloud import bigquery
import urllib.request

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')


#postgres_host = os.environ.get('postgres_host')
#postgres_database = os.environ.get('postgres_database')
#postgres_user = os.environ.get('postgres_user')
#postgres_password = os.environ.get('postgres_password')
#postgres_port = os.environ.get('postgres_port')
#dest_folder = os.environ.get('dest_folder')
postgres_host='localhost'
postgres_database='austin-crime-db'
postgres_user='postgres'
postgres_password='12345678'
postgres_port='5432'
dest_folder='./output'
google_path='le-wagon-bootcamp-365116-b9b42279d20c.json'
project = 'bigquery-public-data'
dataset_id = 'austin_crime'
table_id = 'crime'

#url = "https://raw.githubusercontent.com/dogukannulu/datasets/master/Churn_Modelling.csv"
#destination_path = f'{dest_folder}/churn_modelling.csv' 

destination_path = f'{dest_folder}/{dataset_id}.csv' 

try:
    conn = psycopg2.connect(
        host=postgres_host,
        database=postgres_database,
        user=postgres_user,
        password=postgres_password,
        port=postgres_port
    )
    cur = conn.cursor()
    logging.info('Postgres server connection is successful')
except Exception as e:
    traceback.print_exc()
    logging.error("Couldn't create the Postgres connection")

def download_file_from_big_query(project: str, dataset_id: str, table_id: str, destination_path: str):
    """
    Download Austin crime public dataset from Big Query: saves file in bucket then downloads file locally
    """    

    # Querying the table
    sql_query = (f"""SELECT * FROM {project}.{dataset_id}.{table_id}""")

    # Storing the data in a pandas DataFrame
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=google_path
    client = bigquery.Client()
    try: 
        df = client.query(sql_query).to_dataframe()
        # Saving the DataFrame to a CSV
        df.to_csv(destination_path)
        logging.info(f'dataset {dataset_id} csv file downloaded successfully to {destination_path}')
    except Exception as e:
        logging.error(f'Error while downloading the csv file due to: {e}')
        traceback.print_exc()




def download_file_from_url(url: str, dest_folder: str):
    """
    Download a file from a specific URL and download to the local direcory
    """
    if not os.path.exists(str(dest_folder)):
        os.makedirs(str(dest_folder))  # create folder if it does not exist

    try:
        urllib.request.urlretrieve(url, destination_path)
        logging.info('csv file downloaded successfully to the working directory')
    except Exception as e:
        logging.error(f'Error while downloading the csv file due to: {e}')
        traceback.print_exc()


def create_postgres_table():
    """
    Create the Postgres table with a desired schema
    """
    try:
        cur.execute(f"""CREATE TABLE IF NOT EXISTS {table_id} (Row INTEGER PRIMARY KEY, unique_key INTEGER, 
                    address VARCHAR(100), census_tract FLOAT, clearance_date VARCHAR(50), 
                    clearance_status VARCHAR(50), council_district_code INTEGER, description VARCHAR(100), 
                    district VARCHAR(50), latitude FLOAT, longitude FLOAT, location VARCHAR(50), 
                    location_description VARCHAR(100), primary_type VARCHAR(50), timestamp VARCHAR(50), 
                    x_coordinate INTEGER, y_coordinate INTEGER, year INTEGER, zipcode VARCHAR(50))""")
        
        logging.info(f' New table {table_id} created successfully in postgres server, database {postgres_database}')
    except:
        logging.warning(f' Check if the table {table_id} exists')


def write_to_postgres(destination_path: str):
    """
    Create the dataframe and write to Postgres table if it doesn't already exist
    """
    df = pd.read_csv(f'{destination_path}')
    inserted_row_count = 0

    for _, row in df.iterrows():
        breakpoint()
        count_query = f"""SELECT COUNT(*) FROM {table_id} WHERE row = {row['Row']}"""
        cur.execute(count_query)
        result = cur.fetchone()
        
        if result[0] == 0:
            inserted_row_count += 1
            cur.execute(f"""INSERT INTO {table_id} (Row, unique_key, address, census_tract, clearance_date, 
                        clearance_status, council_district_code, description, district, latitude, longitude, 
                        location, location_description, primary_type, timestamp, x_coordinate, y_coordinate,
                        year, zipcode) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""", 
            (int(row[0]), int(row[1]), str(row[2]), float(row[3]), str(row[4]), str(row[5]), int(row[6]), str(row[7]), str(row[8]), float(row[9]), float(row[10]), str(row[11]), str(row[12]), str(row[13]), str(row[14]), int(row[15]), int(row[16]), int(row[17]), str(row[18])))

    logging.info(f' {inserted_row_count} rows from csv file inserted into {table_id} table successfully')

def write_csv_from_big_query_to_postgres_main():
    download_file_from_big_query (project, dataset_id, table_id, destination_path)
    create_postgres_table()
    write_to_postgres(destination_path)
    conn.commit()
    cur.close()
    conn.close()



if __name__ == '__main__':
    download_file_from_big_query (project, dataset_id, table_id, destination_path)
    create_postgres_table()
    write_to_postgres(destination_path)
    conn.commit()
    cur.close()
    conn.close()