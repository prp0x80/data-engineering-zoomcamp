import os
import argparse
from sqlalchemy import create_engine
import pandas as pd
import pyarrow.parquet as pq


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    file_format = params.file_format
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")
    output_filename = f"output.{file_format}"

    download_data(url, output_filename)

    if file_format == "parquet":
        parquet_to_sql(engine, table_name, output_filename)
    elif file_format == "csv":
        csv_to_sql(engine, table_name, output_filename)
    else:
        print("Invalid file format")
        return

    ZONES_CSV_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv"
    df_zones = pd.read_csv(ZONES_CSV_URL)
    df_zones.to_sql(name="zones", con=engine, if_exists="replace", index=False)


def download_data(url, output_filename):
    os.system(f"wget {url} -O {output_filename}")


def parquet_to_sql(engine, table_name, output_filename):
    parquet_file = pq.ParquetFile(output_filename)
    first_batch = True
    for batch in parquet_file.iter_batches():
        batch_df = batch.to_pandas()
        if first_batch:
            batch_df.head(n=0).to_sql(name=table_name, con=engine, if_exists="replace")
            first_batch = False
        batch_df.to_sql(name=table_name, con=engine, if_exists="append")


def csv_to_sql(engine, table_name, output_filename):
    first_batch = True
    for batch in pd.read_csv(output_filename, compression="gzip", chunksize=10000):
        batch["lpep_pickup_datetime"] = pd.to_datetime(batch["lpep_pickup_datetime"])
        batch["lpep_dropoff_datetime"] = pd.to_datetime(batch["lpep_dropoff_datetime"])
        if first_batch:
            batch.head(n=0).to_sql(name=table_name, con=engine, if_exists="replace")
            first_batch = False
        batch.to_sql(name=table_name, con=engine, if_exists="append")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest CSV data to Postgres")
    parser.add_argument("--user", help="user name for postgres")
    parser.add_argument("--password", help="password for postgres")
    parser.add_argument("--host", help="host for postgres")
    parser.add_argument("--port", help="port for postgres")
    parser.add_argument("--db", help="db name for postgres")
    parser.add_argument(
        "--table_name", help="name of the table where we will write results to"
    )
    parser.add_argument("--url", help="url of the parquet file")
    parser.add_argument("--file_format", help="file format of input data")
    args = parser.parse_args()
    main(args)
