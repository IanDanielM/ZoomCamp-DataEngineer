import os

import pandas as pd
from sqlalchemy import create_engine


# download files from url
def download_file(url, path):
    if url.endswith('.csv.gz'):
        csv_name = f'{path}data.csv.gz'
    else:
        csv_name = f'{path}data.csv'
    os.system(f"wget {url} -O {csv_name}")


# create db connection
def create_db_connection(user, password, host, port, db):
    # create sqlalchemy engine
    engine = create_engine(
        f'postgresql://{user}:{password}@{host}:{port}/{db}')
    return engine


# read csv file
def read_csv_file(file_path, url, chunksize=5000):
    # contains large number of rows
    if not os.path.isfile(file_path):
        download_file(url, path='files/')
    df_iter = pd.read_csv(file_path, iterator=True, chunksize=chunksize)
    return df_iter


# transform the data
def transform_data(df_chunk):
    # transform the data
    if 'lpep_pickup_datetime' in df_chunk.columns:
        df_chunk['lpep_pickup_datetime'] = pd.to_datetime(
            df_chunk['lpep_pickup_datetime'])
    if 'lpep_dropoff_datetime' in df_chunk.columns:
        df_chunk['lpep_dropoff_datetime'] = pd.to_datetime(
            df_chunk['lpep_dropoff_datetime'])
    return df_chunk


# load the data
def load_data(df_chunk, engine, table_name, if_exists='append'):
    df_chunk.to_sql(table_name, engine, if_exists=if_exists, index=False)


# main function to process the data
def process_data(file_path, db_params, table_name, url):
    engine = create_db_connection(**db_params)
    df_iter = read_csv_file(file_path, url)
    for i, df_chunk in enumerate(df_iter):
        df_chunk = transform_data(df_chunk)
        if i == 0:
            load_data(df_chunk, engine, table_name, if_exists='replace')
        else:
            load_data(df_chunk, engine, table_name, if_exists='append')
