## How to run a prefect flow ?
```bash
python flow.py \
    --taxi-color yellow \
    --years 2021 \
    --months 1 2 3 \
    --data-lake gcs-data-lake \
    --creds gcs-data-lake-creds \
    --project-id "dtc-de-375601" \
    --dataset trips_data_all \
    --table rides
```


## Parametrizing a flow
```yaml
...
parameters:
  config:
    taxi-color: yellow
    years:
    - 2019
    months:
    - 1
    - 2
    - 3
    data-lake: gcs-data-lake
    credentials: gcs-data-lake-creds
    project-id: dtc-de-375601
    dataset: trips_data_all
    table: rides
...
```
