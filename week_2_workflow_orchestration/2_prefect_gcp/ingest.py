import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket


@flow(name="ETL to GCS")
def main():
    """
    Do ETL operation from source to GCS.
    """
    color = "yellow"
    year = 2021
    month = 1

    data_file = f"{color}_tripdata_{year}-{month:02d}"
    source = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{data_file}.csv.gz"

    raw_df = extract(source)
    df = transform(raw_df)

    # Sub flow to save file to disk
    file_path = save_file(df, data_file)

    destination = f"data/{color}/{data_file}.parquet"
    load(file_path, destination)


@flow(name="store data", log_prints=True)
def save_file(df, data_file):
    """
    Save a parquet file to disk for visualization.
    """
    print("\tPersisting file onto a disk")
    file_path = f"data/raw/{data_file}.parquet"
    df.to_parquet(file_path, compression="gzip")

    return file_path


@task(log_prints=True, retries=3)
def extract(source):
    """
    Load data from source into pandas DataFrame.
    """
    print("Loading data from source...")
    raw_df = pd.read_csv(source, parse_dates=["tpep_pickup_datetime", "tpep_dropoff_datetime"])

    return raw_df


@task(log_prints=True)
def transform(raw_df):
    """
    Pre-processing data before dumping into a data lake.
    """
    print("Pre-processing data before entering into a data lake")

    print("\tChecking data types")
    print(raw_df.info())
    df = raw_df.copy()

    return df


@task(log_prints=True)
def load(file_path, destination):
    """
    Load transformed data to GCS.
    """
    # Build up Prefect GCS block
    conn = GcsBucket.load("gcs-data-lake")
    
    # Upload data to GCS
    print("Ingesting data to GCS...")
    conn.upload_from_path(file_path, destination)


if __name__ == "__main__":
    main()