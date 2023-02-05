import argparse
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
    parser = argparse.ArgumentParser(description="Ingest data from source to database")

    # Source args
    parser.add_argument("--taxi-color", dest="taxi-color", default="yellow", help="Color of NY taxi data to be extracted")
    parser.add_argument("--years", dest="years", nargs='+', default=[2019], type=int, help="Years of NY taxi data to be extracted")
    parser.add_argument("--months", dest="months", nargs='+', default=[1], type=int, help="Months of NY taxi data to be extracted")
    
    # Destination args
    parser.add_argument("--data-lake", dest="data-lake", required=True, help="Block connection to represent a data lake")
    parser.add_argument("--creds", dest="credentials", required=True, help="Block connection to represent GCP credentials")
    parser.add_argument("--project-id", dest="project-id", required=True, help="Project ID in GCP to write data to")
    parser.add_argument("--dataset", dest="dataset", required=True, help="Dataset in BQ to write data to")
    parser.add_argument("--table", dest="table", required=True, help="Table in BQ to write data to")

    args = parser.parse_args()
    config = args.__dict__
    main(config)
