-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `dataengineering-412009.nytaxi.external_green_tripdata`
OPTIONS
(
  format = 'parquet',
  uris = ['gs://daniel-demage/2022/green_tripdata_2022-*.parquet']
);

-- Count the number of rows in the external_green_tripdata table
SELECT count(*)
FROM `dataengineering-412009.nytaxi.external_green_tripdata`;

-- Create a nonpartitioned_green_tripdata table and populate it with data from the external_green_tripdata table
CREATE OR REPLACE TABLE `dataengineering-412009.nytaxi.nonpartitioned_green_tripdata`
AS
SELECT *
FROM `dataengineering
-412009.nytaxi.external_green_tripdata`;

-- Count the number of distinct PULocationID values in the external_green_tripdata table
SELECT COUNT(DISTINCT PULocationID)
FROM `dataengineering
-412009.nytaxi.external_green_tripdata`;

-- Count the number of distinct PULocationID values in the nonpartitioned_green_tripdata table
SELECT COUNT(DISTINCT PULocationID)
FROM `dataengineering
-412009.nytaxi.nonpartitioned_green_tripdata`;

-- Count the number of rows in the external_green_tripdata table
FROM `dataengineering
-412009.nytaxi.external_green_tripdata`
WHERE fare_amount = 0;

-- Create a partitioned_green_tripdata table 
CREATE TABLE `dataengineering-412009.nytaxi.optimized_green_tripdata`
PARTITION BY DATE (lpep_pickup_datetime)
CLUSTER BY PUlocationID
AS
SELECT *
FROM `dataengineering-412009.nytaxi.nonpartitioned_green_tripdata`;

-- Get the distinct PULocationID values from the nonpartitioned_green_tripdata table
-- where lpep_pickup_datetime is between '2022-06-01' and '2022-06-30'
SELECT DISTINCT PULocationID
FROM `dataengineering-412009.nytaxi.nonpartitioned_green_tripdata`
WHERE DATE
(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';

-- Get the distinct PULocationID values from the optimized_green_tripdata table
SELECT DISTINCT PULocationID FROM `dataengineering-412009.nytaxi.optimized_green_tripdata`
WHERE DATE (lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';