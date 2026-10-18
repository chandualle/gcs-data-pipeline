-- 1. Total records and negative values
SELECT
    COUNT(*) AS total_rows,
    COUNTIF(fare_amount < 0) AS negative_fares,
    COUNTIF(trip_distance < 0) AS negative_distances,
    COUNTIF(total_amount < 0) AS negative_totals
FROM `qwiklabs-gcp-03-0ba9de6d12bc.nyc_taxi.yellow_trips`;

-- 2. Invalid trip timestamps
SELECT
    COUNTIF(
        tpep_pickup_datetime IS NULL
        OR tpep_dropoff_datetime IS NULL
        OR tpep_dropoff_datetime <= tpep_pickup_datetime
    ) AS invalid_timestamp_rows
FROM `qwiklabs-gcp-03-0ba9de6d12bc.nyc_taxi.yellow_trips`;

-- 3. Duplicate records
SELECT
    COUNT(*) - COUNT(DISTINCT TO_JSON_STRING(t)) AS duplicate_rows
FROM `qwiklabs-gcp-03-0ba9de6d12bc.nyc_taxi.yellow_trips` AS t;
