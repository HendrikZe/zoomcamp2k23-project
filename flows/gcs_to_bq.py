from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(retries=3)
def extract_from_gcs(year: int) -> Path:
    """Download single atp matches data from GCS"""
    gcs_path = f"data/atp-single-matches/{year}.parquet"
    gcs_block = GcsBucket.load("gcs-zoomcamp-project")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"./data/gcs/")
    return Path(f"./data/gcs/{gcs_path}")


@task()
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning"""
    df = pd.read_parquet(path)
    return df


@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")

    df.to_gbq(
        destination_table="atp_matches.single_matches",
        project_id="adroit-dock-375317",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )


@flow()
def etl_gcs_to_bq():
    """Main ETL flow to load data into Big Query"""
    year = 2000

    path = extract_from_gcs(year)
    print(path)
    df = transform(path)
    write_bq(df)


if __name__ == "__main__":
    etl_gcs_to_bq()