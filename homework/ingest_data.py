import argparse

import os
from time import time

import pandas as pd
from sqlalchemy import create_engine

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    #url = params.url
    trips_csv = 'trips.csv'
    zones_csv = 'zones.csv'

    trips_url = 'https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2021-01.csv'
    zone_url = 'https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv'

    # Download csv
    os.system(f"wget {trips_url} -O {trips_csv}")
    os.system(f"wget {zone_url} -O {zones_csv}")

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    zone_df = pd.read_csv(zone_url)
    zone_df.to_sql(name='zones', con=engine, if_exists='replace')

    header_row = pd.read_csv(trips_csv, nrows = 0)
    header_row.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    for df in pd.read_csv(trips_csv, iterator= True, chunksize = 100000):

        t_start = time()
        
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        
        df.to_sql(name=table_name, con=engine, if_exists='append')
        
        t_end = time()
        
        print('Inserted another chunk..., took %.3f seconds' % (t_end - t_start))

if __name__ == '__main__':    

    parser = argparse.ArgumentParser(description='Ingest csv data to Postgres')

    parser.add_argument('--user', help='username for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='name of table to write results to')
    #parser.add_argument('--url', help='url of the csv file')

    args = parser.parse_args()
    
    main(args)
