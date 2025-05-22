-- models/analysis/high_value_by_hour.sql
SELECT
  EXTRACT(HOUR FROM f.tpep_pickup_datetime) AS pickup_hour,
  COUNT(*) AS potential_high_value_clients
FROM
  {{ ref('fact_taxi_trips_parquet') }} f
WHERE
  f.passenger_count IS NOT NULL
GROUP BY
  EXTRACT(HOUR FROM f.tpep_pickup_datetime)
ORDER BY
  potential_high_value_clients DESC