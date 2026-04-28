import click
import pandas as pd
from tqdm.auto import tqdm
from sqlalchemy import create_engine


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


@click.command()
@click.option("--pg-user", default="root", help="PostgreSQL user")
@click.option("--pg-pass", default="root", help="PostgreSQL password")
@click.option("--pg-host", default="localhost", help="PostgreSQL host")
@click.option("--pg-db", default="ny_taxi", help="PostgreSQL database name")
@click.option("--pg-port", default="5432", help="PostgreSQL port")
@click.option("--year", default=2021, help="Year of the data to ingest")
@click.option("--month", default=1, help="Month of the data to ingest")
@click.option("--target-table", default="yellow_taxi_data", help="Target table name")
@click.option("--chunksize", default=100000, help="Chunk size for reading CSV")
def run(pg_user, pg_pass, pg_host, pg_db, pg_port, year, month, target_table, chunksize):

    # Read a sample of the data
    prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'
    url = f'{prefix}yellow_tripdata_{year}-{month:02d}.csv.gz'
    df = pd.read_csv(url)

    engine = create_engine(f'postgresql+psycopg://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

    print(pd.io.sql.get_schema(df, name=target_table, con=engine))




    df_iter = pd.read_csv(
       url,
       dtype=dtype,
       parse_dates=parse_dates,
       iterator=True,
       chunksize=chunksize
    )

    first = True

    for df_chunk in df_iter:

        if first:
            #Create table schema (no data)
            df_chunk.head(0).to_sql(
                name=target_table,
                con=engine,
                if_exists="replace"
            )
            first = False
            print("Table created")

            #Insert chunk
            df_chunk.to_sql(
                name=target_table,
                con=engine,
                if_exists="append"
            )

            print("Inserted:", len(df_chunk))


if __name__ == "__main__":
    run() 




"""
docker run -it 
  --network=pg-network 
  taxi_ingest:v001 
    --pg-user=root 
    --pg-pass=root 
    --pg-host=pgdatabase 
    --pg-port=5432 
    --pg-db=ny_taxi 
    --target-table=yellow_taxi_trips
"""