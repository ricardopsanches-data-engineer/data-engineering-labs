#!/usr/bin/env python
# coding: utf-8

import click
import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm

DTYPE = {
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

PARSE_DATES = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

@click.command()
@click.option('--pg_user', default='root', help='PostgreSQL user')
@click.option('--pg_password', default='root', help='PostgreSQL password')
@click.option('--pg_host', default='localhost', help='PostgreSQL host')
@click.option('--pg_port', default=5432, type=int, help='PostgreSQL port')
@click.option('--pg_db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--target_table', default='yellow_taxi_data', help='Target table name')
def run(pg_user, pg_password, pg_host, pg_port, pg_db, target_table):
    year = 2021
    month = 1

#    pg_user = "root"
#    pg_password = "root"
#    pg_host = "localhost"
#    pg_port = "5433"
#    pg_db = "ny_taxi"
#    target_table = "yellow_taxi_data"

    chunksize = 100000
    csv_file = f"ny_taxi_postgres_data/data/yellow_tripdata_{year}-{month:02d}.csv.gz"
    engine = create_engine(
        f"postgresql+psycopg://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}"
    )

    df_iter = pd.read_csv(
        csv_file,
        compression="gzip",
        iterator=True,
        chunksize=chunksize,
        dtype=DTYPE,
        parse_dates=PARSE_DATES,
        low_memory=False
    )

    first_chunk = next(df_iter)

    first_chunk.head(0).to_sql(
        name=target_table,
        con=engine,
        if_exists="replace",
        index=False
    )

    first_chunk.to_sql(
        name=target_table,
        con=engine,
        if_exists="append",
        index=False
    )
    print("primeiro chunk inserido")

    for df_chunk in df_iter:
        df_chunk.to_sql(
            name=target_table,
            con=engine,
            if_exists="append",
            index=False
        )
        print("chunk inserido")

    result = pd.read_sql(f"SELECT COUNT(*) AS total FROM {target_table}", con=engine)
    total = result.iloc[0, 0]
    print(f"Total rows at table: {total}")

if __name__ == "__main__":
    run()