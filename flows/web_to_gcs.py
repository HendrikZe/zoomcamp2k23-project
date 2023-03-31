from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint
import requests
import io
import os
import subprocess


@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    # if randint(0, 1) > 0:
    #     raise Exception

    print(f'Fetching dataset url -> {dataset_url}')
    r = requests.get(dataset_url)
    pd.DataFrame(io.StringIO(r.text)).to_csv()

    df = pd.read_csv(dataset_url)
    return df



@task(log_prints=True, retries=3)
def extract_data(year:int)-> pd.DataFrame:

    url = f"https://raw.githubusercontent.com/JeffSackmann/tennis_atp/master/atp_matches_{year}.csv"

    csv_file = f"atp_matches_{year}.csv"

    #os.system(f"wget {url} -O ./data/csv/{csv_file}")
    subprocess.run(["wget", "-O", f"./data/csv/{csv_file}", url], check=True)

    df_iter = pd.read_csv(f"./data/csv/{csv_file}", iterator=True, chunksize=100000,error_bad_lines=False)

    df = next(df_iter)

    return df


@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df

@task(log_prints=True)
def clean_local_files():
    """Removes any csv or parquet file in the specified path"""
    # Recursively traverse the directory tree
    for root, dirs, files in os.walk("./data/"):
        for filename in files:
            # Check if the file is a CSV or Parquet file
            if filename.endswith('.csv') or filename.endswith('.parquet'):
                file_path = os.path.join(root, filename)
                # Delete the file
                os.remove(file_path)


@task()
def write_local(df: pd.DataFrame, year:int) -> Path:
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
    matchType = "single"
    start_year = 2000
    current_year = 2023

    for year in range(start_year,current_year+1):
        df = extract_data(year)
        path = write_local(df,year)
        write_gcs(path,"gcs-zoomcamp-project")
        clean_local_files()

if __name__ == "__main__":
    etl_web_to_gcs()
