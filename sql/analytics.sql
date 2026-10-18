
-- Daily taxi trip metrics
SELECT
  DATE(tpep_pickup_datetime) AS pickup_date,
  COUNT(*) AS total_trips,
  ROUND(AVG(trip_distance), 2) AS avg_trip_distance,
  ROUND(SUM(fare_amount), 2) AS total_fares
FROM `qwiklabs-gcp-03-0ba9de6d12bc.nyc_taxi.yellow_trips`
GROUP BY pickup_date
ORDER BY pickup_date;
