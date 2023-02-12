-- create external table using fhv 2019 data
-- CREATE OR REPLACE EXTERNAL TABLE `dtc-de-375601.trips.rides`
-- OPTIONS (
--   format = 'CSV', 
--   uris = ['gs://dtc_data_lake_dtc-de-375601/data/fhv/fhv_tripdata_2019-*.csv.gz']
-- );

-- (optional) question 8: Using a more proper format in data lake such as parquet instead of csv.gz
-- note that column types for all files used in an external table must have the same datatype
-- you can check by counting on this external table
CREATE OR REPLACE EXTERNAL TABLE `dtc-de-375601.trips.rides`
OPTIONS (
  format = 'PARQUET', 
  uris = ['gs://dtc_data_lake_dtc-de-375601/data/fhv/fhv_tripdata_2019-*.parquet']
);

-- create bq table using the previous created external table for fhv 2019 data
CREATE OR REPLACE TABLE `dtc-de-375601.trips.non_partitioned_rides` AS (
  SELECT * FROM `dtc-de-375601.trips.rides`
);

-- question 1: how many records for fhv vehicle on year 2019?
SELECT COUNT(*)
FROM `dtc-de-375601.trips.rides`;

-- question 2: how many unique number of Affiliated_base_number 
-- and what is an estimated amount of processing data that needed to query
-- on the external table vs the bq table?
SELECT COUNT(DISTINCT(Affiliated_base_number))
FROM `dtc-de-375601.trips.rides`;

SELECT COUNT(DISTINCT(Affiliated_base_number))
FROM `dtc-de-375601.trips.non_partitioned_rides`;

-- question 3: how many records that have PUlocationID and DUlocationID left blank?
SELECT COUNT(*)
FROM `dtc-de-375601.trips.non_partitioned_rides`
WHERE PUlocationID IS NULL
AND DOlocationID IS NULL;

-- question 4: what is the best strategy to optimize the table 
-- if a query always filters by pickup_datetime and order by Affiliated_base_number?
CREATE OR REPLACE TABLE `dtc-de-375601.trips.partitioned_clustered_rides`
PARTITION BY DATE(pickup_datetime) 
CLUSTER BY Affiliated_base_number
AS (
  SELECT * FROM `dtc-de-375601.trips.rides`
);

SELECT table_name, partition_id, total_rows
FROM `trips.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'partitioned_clustered_rides'
ORDER BY total_rows DESC;

-- question 5: Using the table from question 4 to retrieve the distinct Affiliated_base_number
-- between pickup_datetime around 03/01/2019 and 03/31/2019 (inclusive) and compare estimated bytes of data
-- that have been processed between optimized table and non-partitioned bq table?
SELECT DISTINCT(Affiliated_base_number)
FROM `dtc-de-375601.trips.non_partitioned_rides`
WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31';

SELECT DISTINCT(Affiliated_base_number)
FROM `dtc-de-375601.trips.partitioned_clustered_rides`
WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31';

-- question 6: where is data stored in the external table you created?
-- if external table source is from GCS, the storage of that table is also GCS

-- question 7: Is it a best practice in big query to always cluster your data?
-- Yes, it is. Besides, it's recommended to partition your data as well
