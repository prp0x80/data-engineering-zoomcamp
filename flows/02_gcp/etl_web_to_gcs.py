import os
from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_gcp.cloud_storage import GcsBucket


@task(retries=3, cache_key_fn=task_input_hash)
def fetch(dataset_url: str) -> pd.DataFrame:
    """
    Read data from web into pandas dataframe
    """
    df = pd.read_csv(dataset_url, compression="gzip")
    return df


@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fix dtypes issues
    """
    try:
        df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
        df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
    except:
        df["lpep_pickup_datetime"] = pd.to_datetime(df["lpep_pickup_datetime"])
        df["lpep_dropoff_datetime"] = pd.to_datetime(df["lpep_dropoff_datetime"])
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {df.shape[0]}")
    return df


@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """
    Writes dataframe out locally as parquet file
    """
    os.makedirs(f"data/{color}", exist_ok=True)
    path = Path(f"data/{color}/{dataset_file}.parquet")
    df.to_parquet(path=path, compression="gzip")
    return path


@task(retries=3)
def write_gcs(path: Path) -> None:
    """
    Upload local parquet file to GCS
    """
    gcs_bucket = GcsBucket.load("zoom-gcs")
    gcs_bucket.upload_from_path(from_path=path, to_path=path)
    return


@flow()
def etl_web_to_gcs(month: int = 11, year: int = 2020, color: str = "green") -> None:
    """
    The main ETL function
    """
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"
    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean, color, dataset_file)
    write_gcs(path)
    print(path)


if __name__ == "__main__":
    color = "green"
    year = 2020
    month = 11
    etl_web_to_gcs(month, year, color)
