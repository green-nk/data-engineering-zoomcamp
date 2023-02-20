-- import fhv taxi data to bq
CREATE OR REPLACE EXTERNAL TABLE `dtc-de-375601.development.fhv`
OPTIONS (
  format = 'PARQUET', 
  uris = ['gs://dtc_data_lake_dtc-de-375601/data/fhv/fhv_tripdata_2019-*.parquet']
);

CREATE OR REPLACE TABLE `dtc-de-375601.trips_data_all.fhv_trips_data` AS (
  SELECT
    CAST(dispatching_base_num AS STRING) AS dispatching_base_num, 
    CAST(pickup_datetime AS TIMESTAMP) AS pickup_datetime, 
    CAST(dropOff_datetime AS TIMESTAMP) AS dropOff_datetime, 
    CAST(PUlocationID AS INT64) AS PUlocationID, 
    CAST(DOlocationID AS INT64) AS DOlocationID, 
    CAST(SR_Flag AS INT64) AS SR_Flag, 
    CAST(Affiliated_base_number AS STRING) AS Affiliated_base_number
  FROM `dtc-de-375601.development.fhv`
);

-- import yellow taxi data to bq
CREATE OR REPLACE EXTERNAL TABLE `dtc-de-375601.development.yellow`
OPTIONS (
  format = 'PARQUET', 
  uris = ['gs://dtc_data_lake_dtc-de-375601/data/yellow/yellow_tripdata_*.parquet']
);

CREATE OR REPLACE TABLE `dtc-de-375601.trips_data_all.yellow_trips_data` AS (
  SELECT
    CAST(VendorID AS INT64) AS VendorID, 
    CAST(tpep_pickup_datetime AS TIMESTAMP) AS tpep_pickup_datetime, 
    CAST(tpep_dropoff_datetime AS TIMESTAMP) AS tpep_dropoff_datetime, 
    CAST(passenger_count AS INT64) AS passenger_count, 
    CAST(trip_distance AS NUMERIC) AS trip_distance, 
    CAST(RatecodeID AS INT64) AS RatecodeID, 
    CAST(store_and_fwd_flag AS STRING) AS store_and_fwd_flag, 
    CAST(PULocationID AS INT64) AS PULocationID, 
    CAST(DOLocationID AS INT64) AS DOLocationID, 
    CAST(payment_type AS INT64) AS payment_type, 
    CAST(fare_amount AS NUMERIC) AS fare_amount, 
    CAST(extra AS NUMERIC) AS extra, 
    CAST(mta_tax AS NUMERIC) AS mta_tax, 
    CAST(tip_amount AS NUMERIC) AS tip_amount, 
    CAST(tolls_amount AS NUMERIC) AS tolls_amount, 
    CAST(improvement_surcharge AS NUMERIC) AS improvement_surcharge, 
    CAST(total_amount AS NUMERIC) AS total_amount, 
    CAST(congestion_surcharge AS NUMERIC) AS congestion_surcharge
  FROM `dtc-de-375601.development.yellow`
);

-- import green taxi data to bq
CREATE OR REPLACE EXTERNAL TABLE `dtc-de-375601.development.green`
OPTIONS (
  format = 'PARQUET', 
  uris = ['gs://dtc_data_lake_dtc-de-375601/data/green/green_tripdata_*.parquet']
);

CREATE OR REPLACE TABLE `dtc-de-375601.trips_data_all.green_trips_data` AS (
  SELECT
    CAST(VendorID AS INT64) AS VendorID, 
    CAST(lpep_pickup_datetime AS TIMESTAMP) AS lpep_pickup_datetime, 
    CAST(lpep_dropoff_datetime AS TIMESTAMP) AS lpep_dropoff_datetime, 
    CAST(store_and_fwd_flag AS STRING) AS store_and_fwd_flag, 
    CAST(RatecodeID AS INT64) AS RatecodeID, 
    CAST(PULocationID AS INT64) AS PULocationID, 
    CAST(DOLocationID AS INT64) AS DOLocationID, 
    CAST(passenger_count AS INT64) AS passenger_count, 
    CAST(trip_distance AS NUMERIC) AS trip_distance, 
    CAST(fare_amount AS NUMERIC) AS fare_amount, 
    CAST(extra AS NUMERIC) AS extra, 
    CAST(mta_tax AS NUMERIC) AS mta_tax, 
    CAST(tip_amount AS NUMERIC) AS tip_amount, 
    CAST(tolls_amount AS NUMERIC) AS tolls_amount, 
    CAST(ehail_fee AS NUMERIC) AS ehail_fee, 
    CAST(improvement_surcharge AS NUMERIC) AS improvement_surcharge, 
    CAST(total_amount AS NUMERIC) AS total_amount, 
    CAST(payment_type AS INT64) AS payment_type, 
    CAST(trip_type AS FLOAT64) AS trip_type, 
    CAST(congestion_surcharge AS NUMERIC) AS congestion_surcharge
  FROM `dtc-de-375601.development.green`
);
