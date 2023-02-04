import pandas as pd
from getpass import getpass
from prefect import task
from prefect.tasks import task_input_hash
from sqlalchemy import create_engine


@task(log_prints=True, retries=3)
def load_data(config):
    """
    Load data directly from the given source.
    """
    source = config["source"]
    datetime_labels = config["datetime-labels"]

    print("Loading data from source...")
    raw_df = pd.read_csv(source, parse_dates=datetime_labels)

    return raw_df


@task(log_prints=True)
def transform_data(raw_df):
    """
    Pre-processing data before ingesting to the database.
    """
    print("Pre-processing data before loading into the database...")
    
    print("\tNeglect missing passenger count")
    df = raw_df.query("passenger_count != 0")

    return df


@task(log_prints=True, cache_key_fn=task_input_hash, cache_expiration=pd.Timedelta(days=1))
def ingest_data(df, engine, config):
    """
    From pre-processing file ingested into the database.
    """
    # Insert each chuck of data into database
    table = config["table"]
    chunksize = config["chunksize"]

    print("Inserting into the database...")
    df.to_sql(name=table, con=engine, if_exists="replace", index=True, index_label="_id", chunksize=chunksize)


@task(log_prints=True, retries=3)
def connect_db(username, hostname, port, database):
    """
    Build up connection string to create an database engine.
    """
    print("Connecting to database...")
    password = getpass()

    connection_string = f"postgresql://{username}:{password}@{hostname}:{port}/{database}"
    engine = create_engine(connection_string)

    # Test connection
    try:
        engine.connect()
    except Exception as e:
        raise Exception(e)
        
    return engine
