import io
import pandas as pd
from prefect import task
from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket
from prefect.tasks import task_input_hash


@task(log_prints=True, retries=3)
def extract_from_source(source, datetime_labels=["tpep_pickup_datetime", "tpep_dropoff_datetime"]):
    """
    Load data from source into pandas DataFrame.
    """
    print("Loading data from source...")
    raw_df = pd.read_csv(source, parse_dates=datetime_labels)

    return raw_df


@task(log_prints=True, retries=3)
def load_to_lake(df, destination, config):
    """
    Load transformed data to GCS.
    """
    # Build up Prefect GCS block
    gcs_block = config["data-lake"]
    conn = GcsBucket.load(gcs_block)
    
    # Create a buffer to store parquet file in memory
    f = io.BytesIO()
    df.to_parquet(f, compression="gzip")
    f.seek(0)

    # Upload data to GCS
    print("Ingesting data to GCS...")
    conn.upload_from_file_object(f, destination)


@task(log_prints=True, retries=3, cache_key_fn=task_input_hash, cache_expiration=pd.Timedelta(days=1))
def extract_from_lake(source, file_path, config):
    """
    Download data from GCS to persist on disk for visualization.
    """
    # Build up Prefect GCS block
    gcs_block = config["data-lake"]
    conn = GcsBucket.load(gcs_block)

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

    print("\tCounting total number of rows")
    print(f"Total number of records: {len(df)}")
    
    return df


@task(log_prints=True, retries=3, cache_key_fn=task_input_hash, cache_expiration=pd.Timedelta(days=1))
def load_to_warehouse(df, destination, config, chunksize=100_000):
    """
    Load transformed data to BQ.
    """
    # Create credentials to connect to BQ
    credentials = config["credentials"]
    creds_block = GcpCredentials.load(credentials)
    service_account = creds_block.get_credentials_from_service_account()

    # Write data to BQ
    project_id = destination[0]
    destination_table = '.'.join(destination[1:])
    df.to_gbq(destination_table, project_id, credentials=service_account, chunksize=chunksize, if_exists="append")