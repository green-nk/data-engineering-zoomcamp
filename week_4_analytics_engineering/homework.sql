-- Q1: How many records in fact_trips model after running with test option is false and filtering only pickup_datetime between 2019 and 2020?
SELECT COUNT(*) FROM `dtc-de-375601.production.fact_trips`
WHERE EXTRACT(YEAR FROM pickup_datetime) BETWEEN 2019 AND 2020

-- Q2: What is the distribution by service type filtering between 2019 and 2020?
-- Using data studio to visualize that 90/10 (Yellow/Green)

-- Q3: How many records in stg_fhv_trips_data after running with test option is false and filtering only pickup_datetime in 2019?
SELECT COUNT(*) FROM `dtc-de-375601.development.stg_fhv_trips_data`
WHERE EXTRACT(YEAR FROM pickup_datetime) = 2019

-- Q4: How many records in fact_fhv_trips after running with test option is false and filtering only pickup_datetime in 2019?
SELECT COUNT(*) FROM `dtc-de-375601.development.fact_fhv_trips`
WHERE EXTRACT(YEAR FROM pickup_datetime) = 2019

-- Q5: What is the month that has the biggest number of rides for fact_fhv_trips?
-- Using data studio to visualize that January which has a very high number of rides (suggested outliers in dataset)