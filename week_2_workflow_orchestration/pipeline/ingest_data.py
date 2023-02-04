import argparse
from utlis import connect_db, load_data, transform_data, ingest_data
from prefect import flow

@flow(name="connectDB", log_prints=True)
def connect(config):
    """
    Sub flow to connect to the database in parallel.
    """
    # Connect to Postgresql database
    username = config["username"]
    hostname = config["hostname"]
    port = config["port"]
    database = config["database"]

    engine = connect_db(username, hostname, port, database)

    return engine


@flow(name="ingest")
def main(config):
    """
    Main flow to ingest data from source into Postgresql database.
    """
    # Sub flow to connect to the database
    engine = connect(config)
    
    # Main flow to extract data from source
    raw_df = load_data(config)

    # Main flow to transform raw data
    df = transform_data(raw_df)

    # Main flow to load data into database
    ingest_data(df, engine, config)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest data from source to database")

    # Source args
    parser.add_argument("-s", "--source", dest="source", required=True, help="Link to source data")
    parser.add_argument("-c", "--chunksize", dest="chunksize", default=100000, help="Number of records to be loaded each time")
    parser.add_argument("--datetime-labels", dest="datetime-labels", nargs='+', default=["tpep_pickup_datetime", "tpep_dropoff_datetime"], help="Datetime column names in the source")

    # Postgresql connection args
    parser.add_argument("--host", dest="hostname", required=True, help="Postgresql host name")
    parser.add_argument("-p", "--port", dest="port", required=True, help="Port to connect to Postgresql")
    parser.add_argument("-u", "--user", dest="username", required=True, help="Username for Postgresql")
    parser.add_argument("-d", "--database", dest="database", required=True, help="Database to connect to ")
    parser.add_argument("-t", "--table", dest="table", required=True, help="Table to save the results")

    args = parser.parse_args()
    config = args.__dict__
    main(config)
