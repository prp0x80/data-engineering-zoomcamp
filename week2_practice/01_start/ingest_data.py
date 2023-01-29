import pandas as pd
from sqlalchemy import create_engine


def extract_data(url: str):
    compression = None
    if url.endswith(".csv.gz"):
        compression = "gzip"

    df = pd.read_csv(url, compression=compression)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    print(f"Total records: {df.shape[0]}")

    return df


def transform_data(df):
    print(f"pre: missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    df = df[df["passenger_count"] != 0]
    print(f"post: missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    return df


def chunker(seq, size):
    return (seq[pos : pos + size] for pos in range(0, len(seq), size))


def load_data(engine, table_name, df):
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists="replace")
    n = 0
    for batch_df in chunker(df, 100000):
        n += 1
        print(f"Pushing batch#{str(n).zfill(2)}")
        batch_df.to_sql(name=table_name, con=engine, if_exists="append")


if __name__ == "__main__":
    user = "root"
    password = "root"
    host = "172.19.0.1"
    db = "ny_taxi"
    port = 5432
    table_name = "yellow_trips"
    csv_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")
    raw_data = extract_data(csv_url)
    data = transform_data(raw_data)
    load_data(engine, table_name, data)
