from prefect import flow
from utils import extract_from_source, load_to_lake, extract_from_lake, transform, load_to_warehouse


@flow(name="Yellow NY Taxi Data Ingestion", log_prints=True)
def main(config):
    """
    Main ETL flow from source to a data warehouse.
    """
    color = config["taxi-color"]
    years = config["years"]
    months = config["months"]
    
    project_id = config["project-id"]
    dataset = config["dataset"]
    table = config["table"]
    destination = [project_id, dataset, table]

    for year in years:
        for month in months:
            print(f"Ingesting {color} NY taxi data in {year}-{month:02d}...")
            data_file = f"{color}/{color}_tripdata_{year}-{month:02d}"

            source = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{data_file}.csv.gz"
            interim = f"data/{data_file}.parquet"
            to_gcs(source, interim, config)
            
            file_path = f"../data/raw/{data_file}.parquet"
            to_bq(interim, destination, file_path, config)


@flow(name="Data dumping to GCS", log_prints=True)
def to_gcs(source, destination, config):
    """
    Dump data from source into a data lake.
    """
    raw_df = extract_from_source(source)
    load_to_lake(raw_df, destination, config)


@flow(name="ETL to BQ", log_prints=True)
def to_bq(source, destination, file_path, config):
    """
    Do ETL from a data lake to a data warehouse.
    """
    raw_df = extract_from_lake(source, file_path, config)
    df = transform(raw_df)
    load_to_warehouse(df, destination, config)


if __name__ == "__main__":
    # Accept parameter from main-deployment.yaml instead
    config = dict()

    main(config)
