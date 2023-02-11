from prefect import flow
from utils import extract_from_source, load_to_lake


@flow(name="FHV NY Taxi Data Ingestion", log_prints=True)
def main(config):
    """
    Main ETL flow from source to a data lake.
    """
    color = config["taxi-color"] 
    years = config["years"]
    months = config["months"]

    for year in years:
        for month in months:
            print(f"Ingesting {color.upper()} NY taxi data in {year}-{month:02d}...")
            data_file = f"{color}/{color}_tripdata_{year}-{month:02d}"

            source = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{data_file}.csv.gz"
            destination = f"data/{data_file}.csv.gz"
            to_gcs(source, destination, config)


@flow(name="Data dumping to GCS", log_prints=True)
def to_gcs(source, destination, config):
    """
    Dump data from source into a data lake.
    """
    raw_df = extract_from_source(source, datetime_labels=["pickup_datetime", "dropOff_datetime"])
    load_to_lake(raw_df, destination, config)


if __name__ == "__main__":
    # Accept parameter from main-deployment.yaml instead
    config = dict()

    main(config)
