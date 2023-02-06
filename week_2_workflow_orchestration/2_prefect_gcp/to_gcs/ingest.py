import io
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket


@flow(name="ETL to GCS")
def main():
    """
    Do ETL operation from source to GCS.
    """
    color = "green"
    year = 2020
    month = 1

    data_file = f"{color}_tripdata_{year}-{month:02d}"
    source = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{data_file}.csv.gz"

    raw_df = extract(source)
    df = transform(raw_df)

    destination = f"data/{color}/{data_file}.parquet"
    load(df, destination)


@task(log_prints=True, retries=3)
def extract(source):
    """
    Load data from source into pandas DataFrame.
    """
    print("Loading data from source...")
    raw_df = pd.read_csv(source, parse_dates=["lpep_pickup_datetime", "lpep_dropoff_datetime"])

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


@task(log_prints=True, retries=3)
def load(df, destination):
    """
    Load transformed data to GCS.
    """
    # Build up Prefect GCS block
    conn = GcsBucket.load("gcs-data-lake")
    
    # Create a buffer to store parquet file in memory
    f = io.BytesIO()
    df.to_parquet(f, compression="gzip")
    f.seek(0)

    # Upload data to GCS
    print("Ingesting data to GCS...")
    conn.upload_from_file_object(f, destination)


if __name__ == "__main__":
    main()