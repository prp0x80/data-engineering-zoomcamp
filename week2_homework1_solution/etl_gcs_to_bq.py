import pandas as pd
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(retries=3, cache_key_fn=task_input_hash)
def extract_from_gcs(color: str, year: int, month: int) -> pd.DataFrame:
    """
    Download trip data from GCS
    """
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=f".")
    df = pd.read_parquet(gcs_path)
    return df


@task()
def write_bq(df: pd.DataFrame) -> None:
    """
    Write dataframe to big query
    """
    gcp_credentials_block = GcpCredentials.load("zoom-gcp-credentials")

    df.to_gbq(
        destination_table="trips_data_all.rides",
        project_id="agile-extension-375516",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )


@flow(log_prints=True)
def etl_gcs_to_bq_flow(
    months: list[int] = [2, 3], year: int = 2019, color: str = "yellow"
):
    """
    Main ETL flow to load data into Biq Query
    """
    data = []
    for month in months:
        month_df = extract_from_gcs(color, year, month)
        data.append(month_df)
    df = pd.concat(data)
    write_bq(df)
    print(f"Total records written: {df.shape[0]}")


if __name__ == "__main__":
    color = "yellow"
    year = 2019
    months = [2, 3]
    etl_gcs_to_bq_flow(months, year, color)
