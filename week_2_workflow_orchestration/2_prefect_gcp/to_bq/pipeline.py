import pandas as pd
from prefect import flow, task
from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket

@flow(name="ETL to BQ")
def main():
    """
    Do ETL operation from GCS to BQ.
    """
    color = "yellow"
    year = 2021
    month = 1

    data_file = f"{color}_tripdata_{year}-{month:02d}"
    source = f"data/{color}/{data_file}.parquet"

    file_path = f"../data/raw/{color}/{data_file}.parquet"
    df = extract(source, file_path)

    df = transform(df)

    destination = ["dtc-de-375601", "trips_data_all", "rides"]
    load(df, destination)


@task(log_prints=True, retries=3)
def extract(source, file_path):
    """
    Download data from GCS to persist on disk for visualization.
    """
    # Build up Prefect GCS block
    conn = GcsBucket.load("gcs-data-lake")

    # Copy file object to store on disk
    conn.download_folder_to_path(from_folder=source, to_folder=file_path)
    df = pd.read_parquet(file_path)
    
    return df


@task(log_prints=True)
def transform(df):
    """
    Pre-processing data before entering into a data warehouse.
    """
    print("Pre-processing data before entering into a data warehouse")

    print("\tCreating a primary key")
    df = df.reset_index().rename({"index": "_id"}, axis=1)

    print("\tImputing missing passenger count")
    df["passenger_count"] = df["passenger_count"].fillna(-1)
    
    return df


@task(log_prints=True, retries=3)
def load(df, destination, chunksize=100_000):
    """
    Load transformed data to BQ.
    """
    # Create credentials to connect to BQ
    creds_block = GcpCredentials.load("gcs-data-lake-creds")
    creds = creds_block.get_credentials_from_service_account()

    # Write data to BQ
    project_id = destination[0]
    destination_table = '.'.join(destination[1:])
    df.to_gbq(destination_table, project_id, credentials=creds, chunksize=chunksize, if_exists="append")


if __name__ == "__main__":
    main()