import io
import pandas as pd
from prefect import task
from prefect_gcp.cloud_storage import GcsBucket
from prefect.tasks import task_input_hash


@task(log_prints=True, retries=3, cache_key_fn=task_input_hash, cache_expiration=pd.Timedelta(days=1))
def extract_from_source(source, datetime_labels=["tpep_pickup_datetime", "tpep_dropoff_datetime"]):
    """
    Load data from source into pandas DataFrame.
    """
    print("Loading data from source...")
    raw_df = pd.read_csv(source, parse_dates=datetime_labels)

    print(f"\tChecking total number of rows")
    print(f"Total number of rows: {len(raw_df)}")

    print(f"\tEnsuring data types in all columns")
    data_types = {
        "dispatching_base_num": "str", 
        "PUlocationID": "float", 
        "DOlocationID": "float", 
        "SR_Flag": "float", 
        "Affiliated_base_number": "str" 
    }
    
    df = raw_df.astype(dtype=data_types)
    print(df.dtypes)

    return df


@task(log_prints=True, retries=3, cache_key_fn=task_input_hash, cache_expiration=pd.Timedelta(days=1))
def load_to_lake(df, destination, config):
    """
    Load raw data to GCS.
    """
    # Build up Prefect GCS block
    gcs_block = config["data-lake"]
    conn = GcsBucket.load(gcs_block)
    
    # Create a buffer to store csv file in memory
    f = io.BytesIO()
    df.to_parquet(f, index=False, compression="gzip")
    f.seek(0)

    # Upload data to GCS
    print("Ingesting data to GCS...")
    conn.upload_from_file_object(f, destination)
