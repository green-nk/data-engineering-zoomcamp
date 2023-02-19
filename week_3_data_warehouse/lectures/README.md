## Overview concepts of data warehouse
### OLAP VS OLTP

### What is a data warehouse ?

### BigQuery as a data warehouse
#### Concepts

#### Costs

#### External table vs BQ table

#### Partitioning vs Clustering

#### BQ Infrastructure

#### BQ Best practices

#### BQ ML

## Miscellaneous
### Data
#### Yellow Taxi Data
[Source](https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/yellow)

<u>Schema</u>
```python
data_types = {
    "VendorID": "float", 
    "passenger_count": "float", 
    "trip_distance": "float", 
    "RatecodeID": "float", 
    "store_and_fwd_flag": "str", 
    "PULocationID": "float", 
    "DOLocationID": "float", 
    "payment_type": "float", 
    "fare_amount": "float", 
    "extra": "float", 
    "mta_tax": "float", 
    "tip_amount": "float", 
    "tolls_amount": "float", 
    "improvement_surcharge": "float", 
    "total_amount": "float", 
    "congestion_surcharge": "float", 
}
```
Note that `tpep_pickup_datetime` and `tpep_dropoff_datetime` are `datetime` dtype.


#### Green Taxi Data
[Source](https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/green)

<u>Schema</u>
```python
data_types = {
    "VendorID": "float", 
    "passenger_count": "float", 
    "trip_distance": "float", 
    "RatecodeID": "float", 
    "store_and_fwd_flag": "str", 
    "PULocationID": "float", 
    "DOLocationID": "float", 
    "payment_type": "float", 
    "fare_amount": "float", 
    "extra": "float", 
    "mta_tax": "float", 
    "tip_amount": "float", 
    "tolls_amount": "float", 
    "improvement_surcharge": "float", 
    "total_amount": "float", 
    "congestion_surcharge": "float", 
    "ehail_fee": "float", 
    "trip_type": "str"
}
```
Note that `lpep_pickup_datetime` and `lpep_dropoff_datetime` are `datetime` dtype.


#### For-hire vehicles (FHV)
[Source](https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/fhv)

<u>Schema</u>
```python
data_types = {
    "dispatching_base_num": "str", 
    "PUlocationID": "float", 
    "DOlocationID": "float", 
    "SR_Flag": "float", 
    "Affiliated_base_number": "str"
}
```
Note that `pickup_datetime` and `dropOff_datetime` are `datetime` dtype.


#### Misc (zone lookup file)
[Source](https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/misc)

<u>Schema</u>
```python
data_types = {
    "LocationID": "float", 
    "Borough": "str", 
    "Zone": "str", 
    "service_zone": "str"
}
```
