import argparse
from utlis import load_data, transform_data, ingest_data
from prefect import flow


@flow(name="ingest")
def main(config):
    """
    Main flow to ingest data from source into Postgresql database.
    """
    # Main flow to extract data from source
    raw_df = load_data(config)

    # Main flow to transform raw data
    df = transform_data(raw_df)

    # Main flow to load data into database
    ingest_data(df, config)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest data from source to database")

    # Source args
    parser.add_argument("-s", "--source", dest="source", required=True, help="Link to source data")
    parser.add_argument("-c", "--chunksize", dest="chunksize", default=100000, help="Number of records to be loaded each time")
    parser.add_argument("--datetime-labels", dest="datetime-labels", nargs='+', default=["tpep_pickup_datetime", "tpep_dropoff_datetime"], help="Datetime column names in the source")

    # Postgresql connection args
    parser.add_argument("-b", "--block", dest="block", required=True, help="Block connection to represent the database")
    parser.add_argument("-t", "--table", dest="table", required=True, help="Table to save the results")
    

    args = parser.parse_args()
    config = args.__dict__
    main(config)
