"""
Reads the Postgres table as a dataframe and creates 4 separate dataframes from main table. 
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



def create_base_df(cur):
    """
    Get base dataframe of Austin crime public dataset
    """
    try:
        cur.execute(f'SELECT * FROM {table_id}') 
    except:
        logging.warning(f' Check if the table {table_id} exists')
        return

    rows = cur.fetchall()

    col_names = [desc[0] for desc in cur.description]
    df = pd.DataFrame(rows, columns=col_names)

    # Clean data 
    df.drop(['Row', 'x_coordinate', 'y_coordinate'], axis = 1, inplace = True)
    df.rename(columns = {'timestamp':'occurred_date'}, inplace = True)
    
    # Create parameters
    df_aux = df[['occurred_date','clearance_date']].dropna()
    avg_clearance_time = pd.DataFrame(df_aux['occurred_date'] - df_aux['clearance_date']).mean()
    df['clearance_time'].fillna(value = avg_clearance_time, inplace = True)
    logging.info(f' Table {table_id} loaded successfully into dataframe from database {postgres_database}')
    return df


def create_df_geo(df):
    """
    Create dataframe from Austin crime public dataset with longitude and latitude data
    """
    df_geo = df.dropna(subset={'latitude', 'longitude'})
    return df_geo
    

def create_crimes_per_hour(df):
    """
    Create dataframe with number of crimes per hour of the day from Austin crime public dataset
    """
    crimes_per_hour = df['occurred_date'].dt.hour.value_counts().sort_index()
    df_crimes_per_hour = crimes_per_hour.reset_index()
    df_crimes_per_hour.columns=['hour', 'number_of_crimes']
    return df_crimes_per_hour


def create_crimes_per_year(df):
    """
    Create dataframe with number of crimes per year from Austin crime public dataset
    """
    crimes_per_year = df['occurred_date'].dt.year.value_counts().sort_index()
    df_crimes_per_year = crimes_per_year.reset_index()
    df_crimes_per_year.columns=['year', 'number_of_crimes']
    return df_crimes_per_year


def top_crimes(df):
    """
    Create dataframe with number of crimes per hour of the day from Austin crime public dataset
    """
    df_top_crimes = df['description'].value_counts().head(25).reset_index()
    df_top_crimes.columns = ['crime_type', 'number_of_crimes']
    return df_top_crimes


def create_new_tables_in_postgres(table_id):
    try:
        cur.execute(f'CREATE TABLE IF NOT EXISTS {table_id}_geo (description VARCHAR(50), district VARCHAR(10), latitude FLOAT, longitude FLOAT, primary_type VARCHAR(50))')
        cur.execute(f'CREATE TABLE IF NOT EXISTS {table_id}_crimes_per_hour (hourday INTEGER, numbercrimes INTEGER)')
        cur.execute(f'CREATE TABLE IF NOT EXISTS {table_id}_crimes_per_year (year INTEGER, numbercrimes INTEGER)')
        cur.execute(f'CREATE TABLE IF NOT EXISTS {table_id}_top_crimes  (crime VARCHAR(100), numbercrimes INTEGER')
        logging.info("4 tables created successfully in Postgres server")
    except Exception as e:
        traceback.print_exc()
        logging.error(f'Tables cannot be created due to: {e}')


def insert_geo_table(df_geo, table_id):
    query = f'INSERT INTO {table_id}_geo (description, district, latitude, longitude, primary_type) VALUES (%s,%s,%s,%s,%s)'
    row_count = 0
    for _, row in df_geo.iterrows():
        values = (row['description'], row['district'], row['latitude'], row['longitude'], row['primary_type'])
        cur.execute(query,values)
        row_count += 1
    
    logging.info(f"{row_count} rows inserted into table {table_id}_geo")


def insert_crimes_per_hour_table(df_crimes_per_hour, table_id):
    query = f'INSERT INTO {table_id}_crimes_per_hour (hourday, numbercrimes) VALUES (%s,%s)'
    row_count = 0
    for _, row in df_crimes_per_hour.iterrows():
        values = (row['hour'], row['number_of_crimes'])
        cur.execute(query,values)
        row_count += 1
    
    logging.info(f"{row_count} rows inserted into table {table_id}_crimes_per_hour")


def insert_crimes_per_year_table(df_crimes_per_year, table_id):
    query = f'INSERT INTO {table_id}_crimes_per_year (year, numbercrimes) VALUES (%s,%s)'
    row_count = 0
    for _, row in df_crimes_per_year.iterrows():
        values = (row['year'], row['number_of_crimes'])
        cur.execute(query,values)
        row_count += 1
    
    logging.info(f"{row_count} rows inserted into table {table_id}_crimes_per_year")


def insert_top_crimes_table(df_top_crimes, table_id):
    query = f'INSERT INTO {table_id}_top_crimes (crime, numbercrimes) VALUES (%s,%s)'
    row_count = 0
    for _, row in df_top_crimes.iterrows():
        values = (row['crime_type'], row['number_of_crimes'])
        cur.execute(query,values)
        row_count += 1
    
    logging.info(f"{row_count} rows inserted into table {table_id}_top_crimes")


def write_dfs_to_postgres_main():
    main_df = create_base_df()
    create_new_tables_in_postgres(table_id)
    df_geo = create_df_geo(main_df)
    df_crimes_per_hour = create_crimes_per_hour(main_df)
    df_crimes_per_year = create_crimes_per_year(main_df)
    df_top_crimes = top_crimes(main_df)
    insert_geo_table(df_geo, table_id)
    insert_crimes_per_hour_table(df_crimes_per_hour, table_id)
    insert_crimes_per_year_table(df_crimes_per_year, table_id)
    insert_top_crimes_table(df_top_crimes, table_id)
    conn.commit()
    cur.close()
    conn.close()


if __name__ == '__main__':
    main_df = create_base_df()
    create_new_tables_in_postgres(table_id)
    df_geo = create_df_geo(main_df)
    df_crimes_per_hour = create_crimes_per_hour(main_df)
    df_crimes_per_year = create_crimes_per_year(main_df)
    df_top_crimes = top_crimes(main_df)
    insert_geo_table(df_geo, table_id)
    insert_crimes_per_hour_table(df_crimes_per_hour, table_id)
    insert_crimes_per_year_table(df_crimes_per_year, table_id)
    insert_top_crimes_table(df_top_crimes, table_id)
    conn.commit()
    cur.close()
    conn.close()