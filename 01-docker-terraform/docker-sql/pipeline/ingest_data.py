#!/usr/bin/env python
# coding: utf-8

import click
import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import pyarrow.parquet as pq

# dtype = {
#     "VendorID": "Int64",
#     "passenger_count": "Int64",
#     "trip_distance": "float64",
#     "RatecodeID": "Int64",
#     "store_and_fwd_flag": "string",
#     "PULocationID": "Int64",
#     "DOLocationID": "Int64",
#     "payment_type": "Int64",
#     "fare_amount": "float64",
#     "extra": "float64",
#     "mta_tax": "float64",
#     "tip_amount": "float64",
#     "tolls_amount": "float64",
#     "improvement_surcharge": "float64",
#     "total_amount": "float64",
#     "congestion_surcharge": "float64"
# }

# parse_dates = [
#     "tpep_pickup_datetime",
#     "tpep_dropoff_datetime"
# ]


@click.command()
@click.option('--file', help='path to the file to ingest', required=True)
@click.option('--file-format', help='format of the file to ingest', required=True)
@click.option('--tripdata-type', default='yellow_trips', help='type of trip data ingested')
@click.option('--pg-user', default='root', help='PostgreSQL user')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default=5432, type=int, help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
# @click.option('--year', default=2021, type=int, help='Year of the data')
# @click.option('--month', default=1, type=int, help='Month of the data')
@click.option('--target-table', help='Target table name', required=True)
@click.option('--chunksize', default=100000, type=int, help='Chunk size for reading CSV')

def run(file, file_format, tripdata_type, pg_user, pg_pass, pg_host, pg_port, pg_db, target_table, chunksize):
    """Ingest NYC taxi data into PostgreSQL database."""
    print("Ingesting data into PostgreSQL...")
    dtype = None
    parse_dates = None
    # prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow'
    # url = f'{prefix}/yellow_tripdata_{year}-{month:02d}.csv.gz'
    # url = csv_file
    # zone data

    # z_prefix =  "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/"
    # df_zone_iter = pd.read_csv( z_prefix+ 'taxi_zone_lookup.csv', iterator=True, chunksize=chunksize)
    if tripdata_type == 'yellow_trips':
        dtype = {
            "VendorID": "Int64",
            "passenger_count": "Int64",
            "trip_distance": "float64",
            "RatecodeID": "Int64",
            "store_and_fwd_flag": "string",
            "PULocationID": "Int64",
            "DOLocationID": "Int64",
            "payment_type": "Int64",
            "fare_amount": "float64",
            "extra": "float64",
            "mta_tax": "float64",
            "tip_amount": "float64",
            "tolls_amount": "float64",
            "improvement_surcharge": "float64",
            "total_amount": "float64",
            "congestion_surcharge": "float64"
        }
        parse_dates = [
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime"
        ]
    
    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')


    kwargs={}
    if dtype:
        kwargs['dtype'] = dtype
    if parse_dates:
        kwargs['parse_dates'] = parse_dates
    # if kwargs:
    if file_format == 'csv':
        df_iter = pd.read_csv(
            file,
            iterator=True,
            chunksize=chunksize,
            **kwargs
        ) 
        
        first = True

        for df_chunk in tqdm(df_iter):
            if first:
                df_chunk.head(0).to_sql(
                    name=target_table,
                    con=engine,
                    if_exists='replace'
                )
                first = False

            df_chunk.to_sql(
                name=target_table,
                con=engine,
                if_exists='append'
            )
                
    elif file_format == 'parquet':
        # df_iter = pd.read_parquet(
        #     file,
        #     engine='pyarrow',
        #     chunksize=chunksize,
        #     **kwargs
        # )
        first = True
        parquet_file = pq.ParquetFile(file)

        for batch in parquet_file.iter_batches(batch_size=chunksize):
            batch_df = batch.to_pandas()
            if first:
                batch_df.head(0).to_sql(
                    name=target_table,
                    con=engine,
                    if_exists='replace'
                )
                first = False

            batch_df.to_sql(
                name=target_table,
                con=engine,
                if_exists='append'
                )

    print("Uploading data to the database... before iteration starts")
    print("Ingestion completed.")

if __name__ == '__main__':
    run()
