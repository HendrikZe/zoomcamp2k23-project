from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
import os
import subprocess


@task(log_prints=True, retries=3)
def extract_data(year: int) -> pd.DataFrame:
    """Get the csv files from the url and save them to a local dir"""
    url = f"https://raw.githubusercontent.com/JeffSackmann/tennis_atp/master/atp_matches_{year}.csv"

    csv_file = f"atp_matches_{year}.csv"

    subprocess.run(["wget", "-O", f"./data/csv/{csv_file}", url], check=True)

    df_iter = pd.read_csv(f"./data/csv/{csv_file}", iterator=True, chunksize=100000, error_bad_lines=False)

    df = next(df_iter)

    return df

@task(log_prints=True)
def clean_local_files(path):
    """Removes any csv or parquet file in the specified path"""
    # Recursively traverse the directory tree
    for root, dirs, files in os.walk(path):
        for filename in files:
            # Check if the file is a CSV or Parquet file
            if filename.endswith('.csv') or filename.endswith('.parquet'):
                file_path = os.path.join(root, filename)
                # Delete the file
                os.remove(file_path)


@task()
def write_local(df: pd.DataFrame, year: int) -> Path:
    """Write DataFrame out locally as parquet file"""
    path = Path(f"./data/parquet/{year}.parquet")
    df.to_parquet(path, compression="gzip")
    return path


@task()
def write_gcs(path: Path, bucket_name: str) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load(bucket_name)
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return


@flow()
def etl_web_to_gcs() -> None:
    """The main ETL function"""
    start_year = 2000
    current_year = 2023

    for year in range(start_year, current_year + 1):
        df = extract_data(year)
        path = write_local(df, year)
        write_gcs(path, "gcs-zoomcamp-project")
        clean_local_files("./data/")


if __name__ == "__main__":
    etl_web_to_gcs()
