import os
import pandas as pd
from getpass import getpass
from sqlalchemy import create_engine
from time import time


def load_data(config):
    """
    Load data directly from the given source.
    """
    source = config["source"]
    
    # Keep source file extension for pandas to load it correctly
    source_file = "source.csv.gz" if source.endswith(".csv.gz") else "../data/raw/csv/source.csv"

    os.system(f"wget {source} -O {source_file}")
    print("...Successfully loaded data from source")

    return source_file


def connect_db(config):
    """
    Build up connection string to create an database engine.
    """
    print("Connecting to database...")
    password = getpass()

    username = config["username"]
    hostname = config["hostname"]
    port = config["port"]
    database = config["database"]

    connection_string = f"postgresql://{username}:{password}@{hostname}:{port}/{database}"
    engine = create_engine(connection_string)

    # Test connection
    try:
        engine.connect()
        print("...Successfully connected to database")
    except Exception as e:
        print(e)
        
    return engine


def ingest_data(source_file, engine, config):
    """
    From raw file ingested into the database.
    """
    # Load source data saved on disk onto memory
    datetime_labels = config["datetime-labels"]
    chucksize = config["chucksize"]
    df_iter = pd.read_csv(source_file, parse_dates=datetime_labels, iterator=True, chunksize=chucksize)

    # Insert each chuck of data into database
    table = config["table"]
    batch = 0
    
    for df_chuck in df_iter:
        t_start = time()
        df_chuck.to_sql(index=False, name=table, con=engine, if_exists="append")
        t_end = time()

        time_used = t_end - t_start
        print(f"\tSuccessfully inserted batch No.{batch}, took {time_used:.3f} seconds")

        batch += 1

    print("...Finished ingesting data into the database")
