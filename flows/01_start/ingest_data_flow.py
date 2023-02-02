import pandas as pd
from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta
from prefect_sqlalchemy import SqlAlchemyConnector


@task(
    log_prints=True,
    tags=["extract"],
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=1),
)
def extract_data(url: str):
    compression = None
    if url.endswith(".csv.gz"):
        compression = "gzip"

    df = pd.read_csv(url, compression=compression)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    print(f"Total records: {df.shape[0]}")

    return df


@task(log_prints=True)
def transform_data(df):
    print(f"pre: missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    df = df[df["passenger_count"] != 0]
    print(f"post: missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    return df


def chunker(seq, size):
    return (seq[pos : pos + size] for pos in range(0, len(seq), size))


@task(log_prints=True, retries=3)
def load_data(table_name, df):
    connection_block = SqlAlchemyConnector.load("postgres-connector")
    with connection_block.get_connection(begin=False) as engine:
        df.head(n=0).to_sql(name=table_name, con=engine, if_exists="replace")
        n = 0
        for batch_df in chunker(df, 100000):
            n += 1
            print(f"Pushing batch#{str(n).zfill(2)}")
            batch_df.to_sql(name=table_name, con=engine, if_exists="append")


@flow(name="Subflow", log_prints=True)
def log_subflow(table_name: str):
    print(f"Logging Subflow for: {table_name}")


@flow(name="Ingest Data")
def main_flow(table_name: str = "yellow_taxi_trips"):
    csv_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"
    log_subflow(table_name)
    raw_data = extract_data(csv_url)
    data = transform_data(raw_data)
    load_data(table_name, data)


if __name__ == "__main__":
    main_flow(table_name="yellow_trips")
